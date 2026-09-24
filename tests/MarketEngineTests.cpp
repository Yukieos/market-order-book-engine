#include "market/Affinity.hpp"
#include "market/CSVReplayConnector.hpp"
#include "market/MultiSymbolBook.hpp"
#include "market/OrderBook.hpp"
#include "market/SpscQueue.hpp"
#include "market/StateChecksum.hpp"
#include "market/Time.hpp"
#include "market/research/Backtest.hpp"
#include "market/research/Features.hpp"
#include "market/itch/ItchFileConnector.hpp"
#include "market/itch/MoldUdp64Connector.hpp"
#include "market/itch/SoupBinTcpConnector.hpp"

#include <cstddef>
#include <fstream>
#include <memory>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <iostream>
#include <map>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

#define CHECK(condition) do { if (!(condition)) throw std::runtime_error("CHECK failed: " #condition); } while (false)

using namespace market;

MarketDataEvent event(EventType type, OrderId id, Side side = Side::Buy,
                      Price price = 100, Quantity quantity = 10,
                      std::uint64_t sequence = 0) {
    return {.sequence = sequence, .type = type, .order_id = id, .side = side,
            .price_ticks = price, .quantity = quantity};
}

class ReferenceBook {
public:
    explicit ReferenceBook(std::size_t capacity) : capacity_(capacity) {}

    ProcessResult process(const MarketDataEvent& input) {
        auto found = orders_.find(input.order_id);
        switch (input.type) {
            case EventType::Add:
                if (input.quantity == 0) return ProcessResult::InvalidQuantity;
                if (found != orders_.end()) return ProcessResult::DuplicateOrderId;
                if (orders_.size() == capacity_) return ProcessResult::CapacityExhausted;
                orders_.emplace(input.order_id, RefOrder{input.side, input.price_ticks, input.quantity, next_priority_++});
                return ProcessResult::Applied;
            case EventType::Modify:
                if (input.quantity == 0) return ProcessResult::InvalidQuantity;
                if (found == orders_.end()) return ProcessResult::OrderNotFound;
                if (found->second.side != input.side || found->second.price != input.price_ticks) {
                    found->second.priority = next_priority_++;
                }
                found->second.side = input.side;
                found->second.price = input.price_ticks;
                found->second.quantity = input.quantity;
                return ProcessResult::Applied;
            case EventType::Cancel:
                if (found == orders_.end()) return ProcessResult::OrderNotFound;
                orders_.erase(found);
                return ProcessResult::Applied;
            case EventType::Reduce:  // same book mechanics as Trade (a partial cancel)
            case EventType::Trade:
                if (input.quantity == 0) return ProcessResult::InvalidQuantity;
                if (found == orders_.end()) return ProcessResult::OrderNotFound;
                if (input.quantity > found->second.quantity) return ProcessResult::InvalidQuantity;
                if (input.quantity == found->second.quantity) orders_.erase(found);
                else found->second.quantity -= input.quantity;
                return ProcessResult::Applied;
            case EventType::Replace: {
                if (input.quantity == 0) return ProcessResult::InvalidQuantity;
                if (found == orders_.end()) return ProcessResult::OrderNotFound;
                const Side side = found->second.side;  // inherit original side
                if (input.new_order_id != input.order_id &&
                    orders_.find(input.new_order_id) != orders_.end()) {
                    return ProcessResult::DuplicateOrderId;
                }
                orders_.erase(found);
                orders_.emplace(input.new_order_id,
                                RefOrder{side, input.price_ticks, input.quantity, next_priority_++});
                return ProcessResult::Applied;
            }
        }
        return ProcessResult::OrderNotFound;
    }

    std::vector<OrderView> orders() const {
        std::vector<OrderView> result;
        for (const auto& [id, order] : orders_) {
            result.push_back({id, order.side, order.price, order.quantity, order.priority});
        }
        std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
            if (left.side != right.side) return left.side == Side::Buy;
            if (left.price_ticks != right.price_ticks) {
                return left.side == Side::Buy ? left.price_ticks > right.price_ticks : left.price_ticks < right.price_ticks;
            }
            return left.priority < right.priority;
        });
        return result;
    }

    std::vector<LevelView> levels() const {
        using Key = std::pair<Side, Price>;
        std::map<Key, LevelView> aggregated;
        for (const auto& [id, order] : orders_) {
            (void)id;
            auto& level = aggregated[{order.side, order.price}];
            level.side = order.side;
            level.price_ticks = order.price;
            level.total_quantity += order.quantity;
            ++level.order_count;
        }
        std::vector<LevelView> result;
        for (const auto& [key, level] : aggregated) { (void)key; result.push_back(level); }
        std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
            if (left.side != right.side) return left.side == Side::Buy;
            return left.side == Side::Buy ? left.price_ticks > right.price_ticks : left.price_ticks < right.price_ticks;
        });
        return result;
    }

    std::uint64_t state_checksum() const {
        const auto snapshot = orders();
        return market::state_checksum(snapshot);
    }

private:
    struct RefOrder { Side side; Price price; Quantity quantity; std::uint64_t priority; };
    std::size_t capacity_;
    std::uint64_t next_priority_{0};
    std::map<OrderId, RefOrder> orders_;
};

OrderRequest request(RequestType type, OrderId id, Side side, Price price, Quantity quantity,
                     OrderType order_type = OrderType::Limit,
                     TimeInForce tif = TimeInForce::GTC) {
    return {type, id, side, price, quantity, order_type, tif};
}

// Collects fills from OrderBook::submit through the allocation-free FillSink.
struct FillCollector {
    std::vector<Fill> fills;
    FillSink sink() {
        return FillSink{this, [](void* ctx, const Fill& f) noexcept {
                            static_cast<FillCollector*>(ctx)->fills.push_back(f);
                        }};
    }
};

// A deliberately simple price-time matcher used as the differential oracle for
// OrderBook::submit. O(n) per fill; correctness over speed.
class ReferenceMatcher {
public:
    explicit ReferenceMatcher(std::size_t capacity) : capacity_(capacity) {}

    SubmitResult submit(const OrderRequest& req, std::vector<Fill>& fills) {
        if (req.type == RequestType::Cancel) {
            auto it = orders_.find(req.id);
            if (it == orders_.end()) return SubmitResult::RejectedInvalid;
            orders_.erase(it);
            return SubmitResult::Canceled;
        }
        if (req.quantity == 0) return SubmitResult::RejectedInvalid;
        const bool may_rest = req.order_type == OrderType::Limit && req.tif == TimeInForce::GTC;
        if (may_rest && orders_.find(req.id) != orders_.end()) return SubmitResult::RejectedDuplicate;
        if (req.tif == TimeInForce::FOK && fillable(req) < req.quantity) return SubmitResult::RejectedFOK;

        Quantity remaining = req.quantity;
        while (remaining > 0) {
            auto maker = best_maker(req);
            if (maker == orders_.end()) break;
            const Quantity traded = std::min(remaining, maker->second.quantity);
            fills.push_back(Fill{req.id, maker->first, req.side, maker->second.price, traded,
                                 fill_sequence_++});
            remaining -= traded;
            if (traded == maker->second.quantity) orders_.erase(maker);
            else maker->second.quantity -= traded;
        }

        const Quantity filled = req.quantity - remaining;
        if (remaining == 0) return SubmitResult::FilledComplete;
        if (may_rest) {
            if (orders_.size() == capacity_) return SubmitResult::RejectedCapacity;
            orders_.emplace(req.id, RefOrder{req.side, req.price_ticks, remaining, next_priority_++});
            return filled == 0 ? SubmitResult::RestedNoFill : SubmitResult::PartialFillRested;
        }
        return filled == 0 ? SubmitResult::NoFillKilled : SubmitResult::PartialFillKilled;
    }

    std::vector<LevelView> levels() const {
        std::map<std::pair<Side, Price>, LevelView> aggregated;
        for (const auto& [id, order] : orders_) {
            (void)id;
            auto& level = aggregated[{order.side, order.price}];
            level.side = order.side;
            level.price_ticks = order.price;
            level.total_quantity += order.quantity;
            ++level.order_count;
        }
        std::vector<LevelView> result;
        for (const auto& [key, level] : aggregated) { (void)key; result.push_back(level); }
        std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
            if (left.side != right.side) return left.side == Side::Buy;
            return left.side == Side::Buy ? left.price_ticks > right.price_ticks
                                          : left.price_ticks < right.price_ticks;
        });
        return result;
    }

private:
    struct RefOrder { Side side; Price price; Quantity quantity; std::uint64_t priority; };

    bool crosses(const OrderRequest& req, Price price) const {
        if (req.order_type == OrderType::Market) return true;
        return req.side == Side::Buy ? req.price_ticks >= price : req.price_ticks <= price;
    }

    Quantity fillable(const OrderRequest& req) const {
        const Side opposite = req.side == Side::Buy ? Side::Sell : Side::Buy;
        Quantity available = 0;
        for (const auto& [id, order] : orders_) {
            (void)id;
            if (order.side == opposite && crosses(req, order.price)) available += order.quantity;
        }
        return available;
    }

    std::map<OrderId, RefOrder>::iterator best_maker(const OrderRequest& req) {
        const Side opposite = req.side == Side::Buy ? Side::Sell : Side::Buy;
        auto best = orders_.end();
        for (auto it = orders_.begin(); it != orders_.end(); ++it) {
            if (it->second.side != opposite || !crosses(req, it->second.price)) continue;
            if (best == orders_.end()) { best = it; continue; }
            const bool better_price = req.side == Side::Buy ? it->second.price < best->second.price
                                                            : it->second.price > best->second.price;
            const bool same_price = it->second.price == best->second.price;
            if (better_price || (same_price && it->second.priority < best->second.priority)) best = it;
        }
        return best;
    }

    std::size_t capacity_;
    std::uint64_t next_priority_{0};
    std::uint64_t fill_sequence_{0};
    std::map<OrderId, RefOrder> orders_;
};

void test_order_book_operations() {
    OrderBook book(8);
    CHECK(book.process_event(event(EventType::Add, 1, Side::Buy, 100, 10)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 2, Side::Buy, 100, 20)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 3, Side::Sell, 103, 7)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 4, Side::Sell, 102, 5)) == ProcessResult::Applied);
    CHECK(book.best_bid()->price_ticks == 100);
    CHECK(book.best_bid()->total_quantity == 30);
    CHECK(book.best_ask()->price_ticks == 102);
    CHECK(book.orders_in_priority()[0].id == 1);

    const auto priority = book.find_order(1)->priority;
    CHECK(book.process_event(event(EventType::Modify, 1, Side::Buy, 100, 12)) == ProcessResult::Applied);
    CHECK(book.find_order(1)->priority == priority);
    CHECK(book.process_event(event(EventType::Trade, 1, Side::Buy, 100, 2)) == ProcessResult::Applied);
    CHECK(book.find_order(1)->quantity == 10);
    CHECK(book.process_event(event(EventType::Cancel, 2)) == ProcessResult::Applied);
    CHECK(book.best_bid()->total_quantity == 10);
}

void test_rejections_and_capacity() {
    OrderBook book(2);
    CHECK(book.process_event(event(EventType::Add, 1)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 1)) == ProcessResult::DuplicateOrderId);
    CHECK(book.process_event(event(EventType::Cancel, 999)) == ProcessResult::OrderNotFound);
    CHECK(book.process_event(event(EventType::Modify, 999)) == ProcessResult::OrderNotFound);
    CHECK(book.process_event(event(EventType::Add, 2)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 3)) == ProcessResult::CapacityExhausted);
    CHECK(book.process_event(event(EventType::Trade, 1, Side::Buy, 100, 11)) == ProcessResult::InvalidQuantity);
    CHECK(book.process_event(event(EventType::Cancel, 1)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 3)) == ProcessResult::Applied);
}

void test_csv_is_deterministic() {
    const auto path = std::filesystem::path(MARKET_TEST_DATA_DIR) / "sample_events.csv";
    CSVReplayConnector first(path);
    CSVReplayConnector second(path);
    MarketDataEvent left;
    MarketDataEvent right;
    std::size_t count = 0;
    while (first.next(left)) {
        CHECK(second.next(right));
        CHECK(left == right);
        ++count;
    }
    CHECK(!second.next(right));
    CHECK(count == 8);
}

void test_differential_after_every_event() {
    const std::vector<MarketDataEvent> events{
        event(EventType::Add, 1, Side::Buy, 100, 10),
        event(EventType::Add, 2, Side::Buy, 100, 20),
        event(EventType::Add, 3, Side::Sell, 103, 8),
        event(EventType::Modify, 2, Side::Buy, 101, 12),
        event(EventType::Trade, 1, Side::Buy, 100, 3),
        event(EventType::Add, 4, Side::Sell, 102, 5),
        event(EventType::Modify, 3, Side::Sell, 102, 9),
        event(EventType::Cancel, 4),
        event(EventType::Trade, 3, Side::Sell, 102, 9),
        event(EventType::Cancel, 999),
        event(EventType::Add, 2, Side::Buy, 99, 1),
    };
    OrderBook optimized(16);
    ReferenceBook reference(16);
    for (const auto& input : events) {
        CHECK(optimized.process_event(input) == reference.process(input));
        CHECK(optimized.orders_in_priority() == reference.orders());
        CHECK(optimized.levels() == reference.levels());
    }
}

void test_randomized_differential() {
    constexpr std::size_t capacity = 32;
    OrderBook optimized(capacity);
    ReferenceBook reference(capacity);
    std::mt19937_64 random(0x5eed1234ULL);
    for (std::uint64_t sequence = 1; sequence <= 20'000; ++sequence) {
        const auto kind = static_cast<EventType>(random() % 4);
        const auto id = static_cast<OrderId>((random() % 48) + 1);
        const auto side = random() % 2 == 0 ? Side::Buy : Side::Sell;
        const auto price = static_cast<Price>(9'990 + random() % 21);
        const auto quantity = static_cast<Quantity>(random() % 20);
        const auto input = event(kind, id, side, price, quantity, sequence);
        CHECK(optimized.process_event(input) == reference.process(input));
        CHECK(optimized.orders_in_priority() == reference.orders());
        CHECK(optimized.levels() == reference.levels());
        if (sequence % 256 == 0) CHECK(optimized.state_checksum() == reference.state_checksum());
    }
    CHECK(optimized.state_checksum() == reference.state_checksum());
}

void test_crossed_book_is_retained() {
    OrderBook book(4);
    CHECK(book.process_event(event(EventType::Add, 1, Side::Sell, 100, 5)) == ProcessResult::Applied);
    CHECK(book.process_event(event(EventType::Add, 2, Side::Buy, 101, 7)) == ProcessResult::Applied);
    CHECK(book.best_bid()->price_ticks == 101);
    CHECK(book.best_ask()->price_ticks == 100);
    CHECK(book.best_bid()->price_ticks >= book.best_ask()->price_ticks);
    CHECK(book.order_count() == 2);
}

void test_checksum_observes_state_changes() {
    OrderBook book(4);
    CHECK(book.process_event(event(EventType::Add, 1, Side::Buy, 100, 5)) == ProcessResult::Applied);
    const auto before = book.state_checksum();
    CHECK(book.process_event(event(EventType::Modify, 1, Side::Buy, 100, 6)) == ProcessResult::Applied);
    CHECK(book.state_checksum() != before);
}

void test_spsc_concurrently() {
    constexpr std::size_t count = 100'000;
    SpscQueue<std::uint64_t, 1024> queue;
    std::atomic<bool> failed{false};
    std::thread producer([&] {
        for (std::uint64_t value = 0; value < count; ++value) {
            while (!queue.try_push(value)) std::this_thread::yield();
        }
    });
    std::thread consumer([&] {
        for (std::uint64_t expected = 0; expected < count; ++expected) {
            std::uint64_t value{};
            while (!queue.try_pop(value)) std::this_thread::yield();
            if (value != expected) failed.store(true, std::memory_order_relaxed);
        }
    });
    producer.join();
    consumer.join();
    CHECK(!failed.load());
    CHECK(queue.empty());
}

void test_matching_limit() {
    OrderBook book(64);
    FillCollector fc;
    CHECK(book.submit(request(RequestType::New, 1, Side::Sell, 102, 5), fc.sink()) == SubmitResult::RestedNoFill);
    CHECK(book.submit(request(RequestType::New, 2, Side::Sell, 102, 3), fc.sink()) == SubmitResult::RestedNoFill);
    CHECK(book.submit(request(RequestType::New, 3, Side::Sell, 103, 10), fc.sink()) == SubmitResult::RestedNoFill);
    CHECK(fc.fills.empty());
    CHECK(book.best_ask()->price_ticks == 102);

    // Aggressive buy @102 x6 fills id1 fully (5) then id2 partially (1).
    CHECK(book.submit(request(RequestType::New, 100, Side::Buy, 102, 6), fc.sink()) == SubmitResult::FilledComplete);
    CHECK(fc.fills.size() == 2);
    CHECK((fc.fills[0] == Fill{100, 1, Side::Buy, 102, 5, 0}));
    CHECK((fc.fills[1] == Fill{100, 2, Side::Buy, 102, 1, 1}));
    CHECK(book.find_order(2)->quantity == 2);
    CHECK(book.best_ask()->total_quantity == 2);

    // Aggressive buy @103 x4 empties id2 @102 then takes 2 from id3 @103.
    fc.fills.clear();
    CHECK(book.submit(request(RequestType::New, 101, Side::Buy, 103, 4), fc.sink()) == SubmitResult::FilledComplete);
    CHECK(fc.fills.size() == 2);
    CHECK(fc.fills[0].maker_id == 2 && fc.fills[0].price_ticks == 102 && fc.fills[0].quantity == 2);
    CHECK(fc.fills[1].maker_id == 3 && fc.fills[1].price_ticks == 103 && fc.fills[1].quantity == 2);
    CHECK(book.best_ask()->price_ticks == 103);
    CHECK(book.best_ask()->total_quantity == 8);
    CHECK(!book.best_bid().has_value());
}

void test_matching_partial_and_rest() {
    OrderBook book(64);
    FillCollector fc;
    book.submit(request(RequestType::New, 1, Side::Sell, 100, 5), fc.sink());
    CHECK(book.submit(request(RequestType::New, 2, Side::Buy, 100, 8), fc.sink()) == SubmitResult::PartialFillRested);
    CHECK(fc.fills.size() == 1);
    CHECK(fc.fills[0].quantity == 5);
    CHECK(!book.best_ask().has_value());
    CHECK(book.best_bid()->price_ticks == 100);
    CHECK(book.best_bid()->total_quantity == 3);
    CHECK(book.find_order(2)->quantity == 3);
}

void test_matching_time_in_force() {
    {  // Market with partial liquidity: fills, remainder killed (never rests).
        OrderBook book(64); FillCollector fc;
        book.submit(request(RequestType::New, 1, Side::Sell, 100, 4), fc.sink());
        CHECK(book.submit(request(RequestType::New, 2, Side::Buy, 0, 10, OrderType::Market), fc.sink()) == SubmitResult::PartialFillKilled);
        CHECK(fc.fills.size() == 1 && fc.fills[0].quantity == 4);
        CHECK(!book.best_ask().has_value() && !book.best_bid().has_value());
    }
    {  // Market with no liquidity.
        OrderBook book(64); FillCollector fc;
        CHECK(book.submit(request(RequestType::New, 1, Side::Buy, 0, 5, OrderType::Market), fc.sink()) == SubmitResult::NoFillKilled);
        CHECK(fc.fills.empty());
    }
    {  // IOC: fill what crosses, kill the rest.
        OrderBook book(64); FillCollector fc;
        book.submit(request(RequestType::New, 1, Side::Sell, 100, 3), fc.sink());
        CHECK(book.submit(request(RequestType::New, 2, Side::Buy, 100, 5, OrderType::Limit, TimeInForce::IOC), fc.sink()) == SubmitResult::PartialFillKilled);
        CHECK(fc.fills.size() == 1 && fc.fills[0].quantity == 3);
        CHECK(!book.best_bid().has_value());
    }
    {  // FOK with insufficient liquidity: reject entirely, no state change.
        OrderBook book(64); FillCollector fc;
        book.submit(request(RequestType::New, 1, Side::Sell, 100, 3), fc.sink());
        CHECK(book.submit(request(RequestType::New, 2, Side::Buy, 100, 5, OrderType::Limit, TimeInForce::FOK), fc.sink()) == SubmitResult::RejectedFOK);
        CHECK(fc.fills.empty());
        CHECK(book.best_ask()->total_quantity == 3);
    }
    {  // FOK with sufficient liquidity across two levels: fully filled.
        OrderBook book(64); FillCollector fc;
        book.submit(request(RequestType::New, 1, Side::Sell, 100, 3), fc.sink());
        book.submit(request(RequestType::New, 2, Side::Sell, 101, 5), fc.sink());
        CHECK(book.submit(request(RequestType::New, 3, Side::Buy, 101, 5, OrderType::Limit, TimeInForce::FOK), fc.sink()) == SubmitResult::FilledComplete);
        CHECK(fc.fills.size() == 2);
        CHECK(fc.fills[0].price_ticks == 100 && fc.fills[0].quantity == 3);
        CHECK(fc.fills[1].price_ticks == 101 && fc.fills[1].quantity == 2);
        CHECK(book.best_ask()->price_ticks == 101 && book.best_ask()->total_quantity == 3);
    }
}

void test_matching_price_time_priority() {
    OrderBook book(64);
    FillCollector fc;
    book.submit(request(RequestType::New, 1, Side::Buy, 100, 5), fc.sink());  // older @100
    book.submit(request(RequestType::New, 2, Side::Buy, 100, 5), fc.sink());  // newer @100
    book.submit(request(RequestType::New, 3, Side::Buy, 101, 4), fc.sink());  // better price
    // Sell x6: must take the better price (id3 @101) first, then the oldest @100 (id1).
    CHECK(book.submit(request(RequestType::New, 100, Side::Sell, 100, 6), fc.sink()) == SubmitResult::FilledComplete);
    CHECK(fc.fills.size() == 2);
    CHECK(fc.fills[0].maker_id == 3 && fc.fills[0].price_ticks == 101 && fc.fills[0].quantity == 4);
    CHECK(fc.fills[1].maker_id == 1 && fc.fills[1].price_ticks == 100 && fc.fills[1].quantity == 2);
    CHECK(book.find_order(1)->quantity == 3);
    CHECK(book.find_order(2)->quantity == 5);
}

void test_matching_randomized_differential() {
    constexpr std::size_t capacity = 512;  // >> working set so capacity never diverges
    OrderBook book(capacity);
    ReferenceMatcher reference(capacity);
    std::vector<Fill> book_fills;
    std::vector<Fill> ref_fills;
    FillSink sink{&book_fills, [](void* ctx, const Fill& f) noexcept {
                      static_cast<std::vector<Fill>*>(ctx)->push_back(f);
                  }};
    std::mt19937_64 random(0x00C0FFEEULL);
    for (std::uint64_t n = 1; n <= 20'000; ++n) {
        const auto side = random() % 2 == 0 ? Side::Buy : Side::Sell;
        if (random() % 5 == 0) {
            const auto cid = static_cast<OrderId>(1 + random() % 200);
            const auto rq = request(RequestType::Cancel, cid, side, 0, 0);
            CHECK(book.submit(rq, sink) == reference.submit(rq, ref_fills));
        } else {
            const auto id = static_cast<OrderId>(1 + random() % 200);
            const auto price = static_cast<Price>(9'995 + random() % 11);
            const auto quantity = static_cast<Quantity>(1 + random() % 10);
            const auto order_type = random() % 8 == 0 ? OrderType::Market : OrderType::Limit;
            const auto roll = random() % 10;
            const auto tif = roll < 7 ? TimeInForce::GTC
                                      : (roll < 9 ? TimeInForce::IOC : TimeInForce::FOK);
            const auto rq = request(RequestType::New, id, side, price, quantity, order_type, tif);
            CHECK(book.submit(rq, sink) == reference.submit(rq, ref_fills));
        }
        if (n % 512 == 0) CHECK(book.levels() == reference.levels());
    }
    CHECK(book_fills == ref_fills);
    CHECK(book.levels() == reference.levels());
}

void test_reduce_and_replace() {
    auto replace_event = event(EventType::Replace, 2, Side::Buy, 99, 8);
    replace_event.new_order_id = 3;
    const std::vector<MarketDataEvent> events{
        event(EventType::Add, 1, Side::Buy, 100, 10),
        event(EventType::Add, 2, Side::Buy, 100, 5),
        event(EventType::Reduce, 1, Side::Buy, 100, 4),  // id1 -> qty 6
        replace_event,                                    // id2 -> id3 @99 qty8, side inherited
    };
    OrderBook optimized(16);
    ReferenceBook reference(16);
    for (const auto& input : events) {
        CHECK(optimized.process_event(input) == reference.process(input));
        CHECK(optimized.orders_in_priority() == reference.orders());
        CHECK(optimized.levels() == reference.levels());
    }
    CHECK(optimized.find_order(1)->quantity == 6);
    CHECK(!optimized.find_order(2).has_value());
    CHECK(optimized.find_order(3)->quantity == 8);
    CHECK(optimized.find_order(3)->side == Side::Buy);
    // Replace of a missing order and duplicate new id are rejected.
    auto missing = event(EventType::Replace, 999, Side::Buy, 99, 1);
    missing.new_order_id = 500;
    CHECK(optimized.process_event(missing) == ProcessResult::OrderNotFound);
    auto dup = event(EventType::Replace, 1, Side::Buy, 99, 1);
    dup.new_order_id = 3;  // 3 already exists
    CHECK(optimized.process_event(dup) == ProcessResult::DuplicateOrderId);
}

// --- ITCH 5.0 round-trip helpers (a minimal big-endian encoder for tests) ---

void put_be(std::vector<std::byte>& out, std::uint64_t value, int bytes) {
    for (int i = bytes - 1; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((value >> (8 * i)) & 0xFFULL));
    }
}

// Encode one book-affecting event as a 2-byte-length-framed ITCH 5.0 message.
// Trade encodes as 'C' (executed with price) when price is set, else 'E'.
void itch_encode(std::vector<std::byte>& out, const MarketDataEvent& e) {
    std::vector<std::byte> msg;
    const auto header = [&](char type) {
        msg.push_back(static_cast<std::byte>(type));
        put_be(msg, 0, 2);                        // stock_locate
        put_be(msg, 0, 2);                        // tracking_number
        put_be(msg, e.exchange_timestamp_ns, 6);  // timestamp
    };
    const auto side_byte = e.side == Side::Buy ? std::byte{'B'} : std::byte{'S'};
    switch (e.type) {
        case EventType::Add:
            header('A');
            put_be(msg, e.order_id, 8);
            msg.push_back(side_byte);
            put_be(msg, e.quantity, 4);
            for (int i = 0; i < 8; ++i) msg.push_back(std::byte{' '});  // stock
            put_be(msg, static_cast<std::uint64_t>(e.price_ticks), 4);
            break;
        case EventType::Trade:
            if (e.price_ticks != 0) {
                header('C');
                put_be(msg, e.order_id, 8);
                put_be(msg, e.quantity, 4);   // executed shares
                put_be(msg, 0, 8);            // match number
                msg.push_back(std::byte{'Y'});// printable
                put_be(msg, static_cast<std::uint64_t>(e.price_ticks), 4);
            } else {
                header('E');
                put_be(msg, e.order_id, 8);
                put_be(msg, e.quantity, 4);   // executed shares
                put_be(msg, 0, 8);            // match number
            }
            break;
        case EventType::Reduce:
            header('X');
            put_be(msg, e.order_id, 8);
            put_be(msg, e.quantity, 4);       // canceled shares
            break;
        case EventType::Cancel:
            header('D');
            put_be(msg, e.order_id, 8);
            break;
        case EventType::Replace:
            header('U');
            put_be(msg, e.order_id, 8);       // original reference
            put_be(msg, e.new_order_id, 8);   // new reference
            put_be(msg, e.quantity, 4);
            put_be(msg, static_cast<std::uint64_t>(e.price_ticks), 4);
            break;
        case EventType::Modify:
            break;  // not an ITCH message
    }
    put_be(out, static_cast<std::uint64_t>(msg.size()), 2);
    out.insert(out.end(), msg.begin(), msg.end());
}

void test_itch_round_trip() {
    auto replace_event = event(EventType::Replace, 1, Side::Buy, 999, 40);
    replace_event.new_order_id = 4;
    const std::vector<MarketDataEvent> script{
        event(EventType::Add, 1, Side::Buy, 1000, 100),
        event(EventType::Add, 2, Side::Buy, 1000, 50),
        event(EventType::Add, 3, Side::Sell, 1020, 80),
        event(EventType::Trade, 1, Side::Buy, 0, 30),      // 'E' partial execution
        event(EventType::Trade, 3, Side::Sell, 1020, 20),  // 'C' execution with price
        event(EventType::Reduce, 2, Side::Buy, 0, 10),     // 'X' partial cancel
        replace_event,                                      // 'U' replace 1 -> 4
        event(EventType::Cancel, 4),                        // 'D' delete
        event(EventType::Trade, 2, Side::Buy, 0, 40),      // 'E' full fill removes order 2
    };

    std::vector<std::byte> bytes;
    for (const auto& e : script) itch_encode(bytes, e);

    itch::ItchFileConnector connector(std::move(bytes));
    OrderBook book(64);
    ReferenceBook reference(64);
    MarketDataEvent decoded{};
    std::size_t index = 0;
    while (connector.next(decoded)) {
        CHECK(index < script.size());
        const auto& expected = script[index];
        CHECK(decoded.type == expected.type);
        CHECK(decoded.order_id == expected.order_id);
        if (expected.type == EventType::Add) {
            CHECK(decoded.side == expected.side);
            CHECK(decoded.price_ticks == expected.price_ticks);
            CHECK(decoded.quantity == expected.quantity);
        }
        if (expected.type == EventType::Replace) {
            CHECK(decoded.new_order_id == expected.new_order_id);
            CHECK(decoded.price_ticks == expected.price_ticks);
            CHECK(decoded.quantity == expected.quantity);
        }
        // Reconstruct from the decoded stream; oracle applies the intended script.
        book.process_event(decoded);
        reference.process(expected);
        CHECK(book.orders_in_priority() == reference.orders());
        CHECK(book.levels() == reference.levels());
        ++index;
    }
    CHECK(!connector.failed());
    CHECK(index == script.size());
    CHECK(connector.messages_decoded() == script.size());
    CHECK(book.state_checksum() == reference.state_checksum());
    // End state: only order 3 remains, sell @1020 with 60 left.
    CHECK(book.best_ask()->price_ticks == 1020);
    CHECK(book.best_ask()->total_quantity == 60);
    CHECK(!book.best_bid().has_value());
}

void test_itch_malformed_and_skipped() {
    {  // Truncated: frame claims 36 bytes but only 1 is present.
        std::vector<std::byte> bytes;
        put_be(bytes, 36, 2);
        bytes.push_back(std::byte{'A'});
        itch::ItchFileConnector connector(std::move(bytes));
        MarketDataEvent e{};
        CHECK(!connector.next(e));
        CHECK(connector.failed());
    }
    {  // Zero-length frame is malformed.
        std::vector<std::byte> bytes;
        put_be(bytes, 0, 2);
        itch::ItchFileConnector connector(std::move(bytes));
        MarketDataEvent e{};
        CHECK(!connector.next(e));
        CHECK(connector.failed());
    }
    {  // A non-book message ('S' system event) is skipped, then a valid Add is returned.
        std::vector<std::byte> bytes;
        std::vector<std::byte> system_message;
        system_message.push_back(std::byte{'S'});
        for (int i = 0; i < 11; ++i) system_message.push_back(std::byte{0});
        put_be(bytes, static_cast<std::uint64_t>(system_message.size()), 2);
        bytes.insert(bytes.end(), system_message.begin(), system_message.end());
        itch_encode(bytes, event(EventType::Add, 7, Side::Sell, 500, 3));

        itch::ItchFileConnector connector(std::move(bytes));
        MarketDataEvent e{};
        CHECK(connector.next(e));
        CHECK(!connector.failed());
        CHECK(e.type == EventType::Add && e.order_id == 7 && e.side == Side::Sell);
        CHECK(e.price_ticks == 500 && e.quantity == 3);
        CHECK(!connector.next(e));
        CHECK(!connector.failed());
    }
}

// --- M3 follow-up: MoldUDP64 framing + gap/overlap detection ---

// A MoldUDP64 packet's message-block region uses the same 2-byte-length framing as
// BinaryFILE, so itch_encode builds it directly.
std::vector<std::byte> mold_packet(std::uint64_t sequence,
                                   const std::vector<MarketDataEvent>& events) {
    std::vector<std::byte> packet;
    for (int i = 0; i < 10; ++i) packet.push_back(std::byte{0});  // session id
    put_be(packet, sequence, 8);
    put_be(packet, static_cast<std::uint64_t>(events.size()), 2);
    for (const auto& e : events) itch_encode(packet, e);  // each block: length + message
    return packet;
}

std::unique_ptr<itch::DatagramSource> in_memory(std::vector<std::vector<std::byte>> datagrams) {
    return std::make_unique<itch::InMemoryDatagramSource>(std::move(datagrams));
}

// The raw ITCH message for one event (itch_encode output with the 2-byte frame removed).
std::vector<std::byte> itch_message_only(const MarketDataEvent& e) {
    std::vector<std::byte> framed;
    itch_encode(framed, e);
    return std::vector<std::byte>(framed.begin() + 2, framed.end());
}

// A retransmit server backed by an in-memory map of sequence -> raw ITCH message.
class InMemoryRetransmit final : public itch::RetransmitSource {
public:
    void add(std::uint64_t sequence, std::vector<std::byte> message) {
        store_[sequence] = std::move(message);
    }
    std::optional<std::span<const std::byte>> recover(std::uint64_t sequence) override {
        const auto it = store_.find(sequence);
        if (it == store_.end()) return std::nullopt;
        return std::span<const std::byte>(it->second);
    }

private:
    std::map<std::uint64_t, std::vector<std::byte>> store_;
};

void test_mold_udp64_basic() {
    const std::vector<MarketDataEvent> first{
        event(EventType::Add, 10, Side::Buy, 1000, 100),
        event(EventType::Add, 11, Side::Sell, 1020, 50),
    };
    const std::vector<MarketDataEvent> second{event(EventType::Add, 12, Side::Buy, 999, 25)};
    std::vector<std::vector<std::byte>> datagrams{
        mold_packet(1, first),
        mold_packet(0, {}),   // heartbeat (count 0): skipped
        mold_packet(3, second),
    };
    itch::MoldUdp64Connector connector(in_memory(std::move(datagrams)));
    MarketDataEvent decoded{};
    std::vector<std::pair<OrderId, std::uint64_t>> got;
    while (connector.next(decoded)) got.emplace_back(decoded.order_id, decoded.sequence);

    CHECK(!connector.failed());
    CHECK(connector.messages_decoded() == 3);
    CHECK(connector.gaps_detected() == 0);
    CHECK(connector.messages_missed() == 0);
    CHECK(got.size() == 3);
    CHECK(got[0].first == 10 && got[0].second == 1);
    CHECK(got[1].first == 11 && got[1].second == 2);
    CHECK(got[2].first == 12 && got[2].second == 3);
}

void test_mold_udp64_gap() {
    std::vector<std::vector<std::byte>> datagrams{
        mold_packet(1, {event(EventType::Add, 1, Side::Buy, 100, 10),
                        event(EventType::Add, 2, Side::Buy, 100, 10)}),  // sequences 1,2
        mold_packet(5, {event(EventType::Add, 3, Side::Buy, 100, 10)}),  // gap: 3,4 missed
    };
    itch::MoldUdp64Connector connector(in_memory(std::move(datagrams)));
    MarketDataEvent decoded{};
    std::size_t count = 0;
    std::uint64_t last_seq = 0;
    while (connector.next(decoded)) { ++count; last_seq = decoded.sequence; }
    CHECK(count == 3);
    CHECK(connector.gaps_detected() == 1);
    CHECK(connector.messages_missed() == 2);  // sequences 3 and 4
    CHECK(last_seq == 5);
}

void test_mold_udp64_overlap() {
    // Second packet retransmits sequences 2,3 and adds 4; only 4 is new.
    std::vector<std::vector<std::byte>> datagrams{
        mold_packet(1, {event(EventType::Add, 1, Side::Buy, 100, 10),
                        event(EventType::Add, 2, Side::Buy, 100, 10),
                        event(EventType::Add, 3, Side::Buy, 100, 10)}),
        mold_packet(2, {event(EventType::Add, 2, Side::Buy, 100, 10),
                        event(EventType::Add, 3, Side::Buy, 100, 10),
                        event(EventType::Add, 4, Side::Buy, 100, 10)}),
    };
    itch::MoldUdp64Connector connector(in_memory(std::move(datagrams)));
    MarketDataEvent decoded{};
    std::vector<OrderId> ids;
    while (connector.next(decoded)) ids.push_back(decoded.order_id);
    CHECK(ids.size() == 4);
    CHECK(ids[0] == 1 && ids[1] == 2 && ids[2] == 3 && ids[3] == 4);
    CHECK(connector.gaps_detected() == 0);
    CHECK(connector.messages_missed() == 0);
}

void test_mold_udp64_retransmit_recovery() {
    const std::vector<MarketDataEvent> all{
        event(EventType::Add, 1, Side::Buy, 100, 10),
        event(EventType::Add, 2, Side::Buy, 100, 10),
        event(EventType::Add, 3, Side::Buy, 100, 10),  // "lost", but recoverable
        event(EventType::Add, 4, Side::Buy, 100, 10),
        event(EventType::Add, 5, Side::Buy, 100, 10),
    };
    std::vector<std::vector<std::byte>> datagrams{
        mold_packet(1, {all[0], all[1]}),   // sequences 1,2
        mold_packet(4, {all[3], all[4]}),   // sequences 4,5 -> gap at 3
    };
    auto retransmit = std::make_unique<InMemoryRetransmit>();
    retransmit->add(3, itch_message_only(all[2]));

    itch::MoldUdp64Connector connector(in_memory(std::move(datagrams)), retransmit.get());
    std::vector<OrderId> ids;
    MarketDataEvent decoded{};
    while (connector.next(decoded)) ids.push_back(decoded.order_id);

    CHECK(ids.size() == 5);
    for (std::size_t i = 0; i < 5; ++i) CHECK(ids[i] == static_cast<OrderId>(i + 1));
    CHECK(connector.gaps_detected() == 1);
    CHECK(connector.messages_missed() == 0);  // fully recovered, gap-free delivery
}

void test_mold_udp64_retransmit_partial() {
    const std::vector<MarketDataEvent> all{
        event(EventType::Add, 1, Side::Buy, 100, 10),
        event(EventType::Add, 2, Side::Buy, 100, 10),
        event(EventType::Add, 3, Side::Buy, 100, 10),
        event(EventType::Add, 4, Side::Buy, 100, 10),
        event(EventType::Add, 5, Side::Buy, 100, 10),
    };
    std::vector<std::vector<std::byte>> datagrams{
        mold_packet(1, {all[0], all[1]}),   // 1,2
        mold_packet(5, {all[4]}),           // 5 -> gap at 3,4
    };
    auto retransmit = std::make_unique<InMemoryRetransmit>();
    retransmit->add(3, itch_message_only(all[2]));  // seq 4 is unrecoverable

    itch::MoldUdp64Connector connector(in_memory(std::move(datagrams)), retransmit.get());
    std::vector<OrderId> ids;
    MarketDataEvent decoded{};
    while (connector.next(decoded)) ids.push_back(decoded.order_id);

    CHECK(ids.size() == 4);  // 1, 2, 3 (recovered), 5
    CHECK(ids[0] == 1 && ids[1] == 2 && ids[2] == 3 && ids[3] == 5);
    CHECK(connector.gaps_detected() == 1);
    CHECK(connector.messages_missed() == 1);  // seq 4 could not be recovered
}

// --- M3 follow-up: streaming file connector matches the in-memory framer ---

void test_itch_streaming_file_equivalence() {
    std::vector<MarketDataEvent> script;
    for (OrderId id = 1; id <= 500; ++id) {
        script.push_back(event(EventType::Add, id, id % 2 == 0 ? Side::Buy : Side::Sell,
                               static_cast<Price>(1000 + (id % 20)), 1 + id % 7));
    }
    std::vector<std::byte> bytes;
    for (const auto& e : script) itch_encode(bytes, e);

    const auto path = std::filesystem::temp_directory_path() /
                      "market_itch_stream_test.bin";
    {
        std::ofstream out(path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }

    std::vector<std::byte> bytes_copy(bytes);
    itch::ItchFileConnector streamed(path);                    // reads from disk in chunks
    itch::ItchFileConnector in_memory_conn(std::move(bytes_copy));  // whole buffer
    MarketDataEvent a{};
    MarketDataEvent b{};
    std::size_t count = 0;
    while (streamed.next(a)) {
        CHECK(in_memory_conn.next(b));
        CHECK(a == b);
        ++count;
    }
    CHECK(!in_memory_conn.next(b));
    CHECK(!streamed.failed() && !in_memory_conn.failed());
    CHECK(count == script.size());
    std::filesystem::remove(path);
}

// --- M3 follow-up: SoupBinTCP framing ---

void soup_packet(std::vector<std::byte>& out, char type, const std::vector<std::byte>& payload) {
    put_be(out, static_cast<std::uint64_t>(payload.size() + 1), 2);  // length = type + payload
    out.push_back(static_cast<std::byte>(type));
    out.insert(out.end(), payload.begin(), payload.end());
}

void test_soupbintcp() {
    auto trade_with_price = event(EventType::Trade, 1, Side::Buy, 100, 4);  // encodes as 'C'
    const std::vector<MarketDataEvent> script{
        event(EventType::Add, 1, Side::Buy, 100, 10),
        event(EventType::Add, 2, Side::Buy, 100, 5),
        event(EventType::Add, 3, Side::Sell, 105, 8),
        trade_with_price,
        event(EventType::Cancel, 2),
    };
    std::vector<std::byte> bytes;
    soup_packet(bytes, 'H', {});                                    // server heartbeat: skipped
    for (const auto& e : script) soup_packet(bytes, 'S', itch_message_only(e));
    soup_packet(bytes, 'H', {});                                    // another heartbeat
    soup_packet(bytes, 'Z', {});                                    // end of session

    itch::SoupBinTcpConnector connector(std::move(bytes));
    OrderBook book(64);
    ReferenceBook reference(64);
    MarketDataEvent decoded{};
    std::size_t index = 0;
    std::uint64_t last_seq = 0;
    while (connector.next(decoded)) {
        CHECK(index < script.size());
        book.process_event(decoded);
        reference.process(script[index]);
        CHECK(book.levels() == reference.levels());
        last_seq = decoded.sequence;
        ++index;
    }
    CHECK(!connector.failed());
    CHECK(connector.end_of_session());
    CHECK(index == script.size());
    CHECK(connector.messages_decoded() == script.size());
    CHECK(last_seq == script.size());  // sequenced packets advance 1..N
    CHECK(book.state_checksum() == reference.state_checksum());
}

void test_soupbintcp_malformed() {
    std::vector<std::byte> bytes;
    put_be(bytes, 0, 2);  // zero-length packet: no room for a type byte
    itch::SoupBinTcpConnector connector(std::move(bytes));
    MarketDataEvent e{};
    CHECK(!connector.next(e));
    CHECK(connector.failed());
}

// --- Phase 1: per-symbol books ---

void test_multi_symbol_routing() {
    MultiSymbolBook multi(64);
    OrderBook ref7(64);
    OrderBook ref42(64);
    const auto apply = [&](SymbolId symbol, EventType type, OrderId id, Side side,
                           Price price, Quantity quantity) {
        MarketDataEvent e = event(type, id, side, price, quantity);
        e.symbol = symbol;
        OrderBook& reference = symbol == 7 ? ref7 : ref42;
        CHECK(multi.process_event(e) == reference.process_event(e));
    };

    apply(7, EventType::Add, 1, Side::Buy, 100, 10);
    apply(42, EventType::Add, 1, Side::Buy, 200, 7);   // same order id, different symbol
    apply(7, EventType::Add, 2, Side::Sell, 105, 5);
    apply(42, EventType::Add, 3, Side::Sell, 210, 4);
    apply(7, EventType::Add, 3, Side::Buy, 101, 8);
    apply(42, EventType::Cancel, 1, Side::Buy, 0, 0);  // remove symbol 42's only bid

    CHECK(multi.symbol_count() == 2);
    CHECK(multi.book(7)->levels() == ref7.levels());
    CHECK(multi.book(42)->levels() == ref42.levels());
    CHECK(multi.book(99) == nullptr);  // never seen

    // Per-symbol BBO is independent (this is the whole point of Phase 1).
    CHECK(multi.best_bid(7)->price_ticks == 101);
    CHECK(multi.best_ask(7)->price_ticks == 105);
    CHECK(!multi.best_bid(42).has_value());
    CHECK(multi.best_ask(42)->price_ticks == 210);

    CHECK(multi.total_orders() == ref7.order_count() + ref42.order_count());
    CHECK(multi.state_checksum() ==
          (checksum_mix(7) ^ ref7.state_checksum() ^ checksum_mix(42) ^ ref42.state_checksum()));

    // Out-of-range symbol is rejected, not routed.
    MarketDataEvent bad = event(EventType::Add, 5, Side::Buy, 100, 1);
    bad.symbol = 1u << 16;  // == max_symbols
    MultiSymbolBook small(64, 1u << 16);
    CHECK(small.process_event(bad) == ProcessResult::CapacityExhausted);
    CHECK(small.symbol_count() == 0);
}

// --- Phase 2: L1 feature extraction (post-event, leakage-proof) ---

void test_l1_features() {
    OrderBook book(16);
    book.process_event(event(EventType::Add, 1, Side::Buy, 100, 10));
    const auto r1 = research::l1_row(1, 1000, book);
    CHECK(!r1.two_sided);  // only a bid so far
    CHECK(r1.bid_px == 100 && r1.bid_sz == 10 && r1.ask_px == 0 && r1.ask_sz == 0);

    book.process_event(event(EventType::Add, 2, Side::Sell, 105, 5));
    const auto r2 = research::l1_row(2, 2000, book);
    CHECK(r2.two_sided);
    CHECK(r2.bid_px == 100 && r2.bid_sz == 10 && r2.ask_px == 105 && r2.ask_sz == 5);
    CHECK(research::l1_changed(r1, r2));

    book.process_event(event(EventType::Add, 3, Side::Buy, 100, 10));  // deepen best bid
    const auto r3 = research::l1_row(3, 3000, book);
    CHECK(r3.bid_sz == 20);
    CHECK(research::l1_changed(r2, r3));

    // Same L1 state at a later event: not a change (sampling clock is L1 updates).
    const auto r3_again = research::l1_row(4, 4000, book);
    CHECK(!research::l1_changed(r3, r3_again));
}

// --- Phase 3: event-driven backtest loop + time model ---

MarketDataEvent sym_event(SymbolId symbol, EventType type, OrderId id, Side side, Price price,
                          Quantity quantity, std::uint64_t timestamp_ns) {
    MarketDataEvent e = event(type, id, side, price, quantity);
    e.symbol = symbol;
    e.exchange_timestamp_ns = timestamp_ns;
    return e;
}

void test_backtest_handworked() {
    research::BacktestConfig cfg;
    cfg.locate = 5;
    cfg.latency_ns = 0;  // frictionless-immediate upper bound
    cfg.order_size = 1;
    cfg.enter_threshold = 0.30;
    cfg.exit_threshold = 0.10;
    research::Backtester bt(cfg, 64);

    bt.on_event(sym_event(5, EventType::Add, 1, Side::Buy, 100, 10, 1000));   // one-sided
    bt.on_event(sym_event(5, EventType::Add, 2, Side::Sell, 101, 5, 1010));   // imb +0.33 -> Long, buy1@101
    bt.on_event(sym_event(5, EventType::Add, 3, Side::Sell, 101, 95, 1020));  // imb -0.82 -> Short, sell2@100
    bt.on_event(sym_event(5, EventType::Add, 4, Side::Buy, 100, 90, 1030));   // imb 0 -> Flat, buy1@101
    const auto r = bt.finish();

    CHECK(r.trades == 3);
    CHECK(r.position == 0);
    CHECK(r.cash_ticks == -2);       // -101 + 200 - 101
    CHECK(r.final_pnl_2x == -4);     // flat, so realized PnL = -2 ticks
    CHECK(r.unfilled == 0);
    const auto& f = bt.fills();
    CHECK(f.size() == 3);
    CHECK(f[0].side == Side::Buy && f[0].price == 101 && f[0].quantity == 1);
    CHECK(f[1].side == Side::Sell && f[1].price == 100 && f[1].quantity == 2);
    CHECK(f[2].side == Side::Buy && f[2].price == 101 && f[2].quantity == 1);
}

void test_backtest_latency_delays_fill() {
    research::BacktestConfig cfg;
    cfg.locate = 5;
    cfg.latency_ns = 100;
    cfg.order_size = 1;
    research::Backtester bt(cfg, 64);

    bt.on_event(sym_event(5, EventType::Add, 1, Side::Buy, 100, 10, 1000));
    bt.on_event(sym_event(5, EventType::Add, 2, Side::Sell, 101, 5, 1000));  // decide Long, arrival=1100
    CHECK(bt.fills().empty());                                                // not arrived
    bt.on_event(sym_event(5, EventType::Add, 3, Side::Buy, 100, 1, 1050));    // ts 1050 < 1100
    CHECK(bt.fills().empty());
    bt.on_event(sym_event(5, EventType::Add, 4, Side::Buy, 100, 1, 1200));    // ts 1200 >= 1100 -> release
    CHECK(bt.fills().size() == 1);
    CHECK(bt.fills()[0].timestamp_ns == 1100);  // stamped at arrival, not decision
    CHECK(bt.fills()[0].side == Side::Buy && bt.fills()[0].price == 101);
}

void test_backtest_determinism() {
    const auto run = [] {
        research::BacktestConfig cfg;
        cfg.locate = 5;
        cfg.latency_ns = 50;
        research::Backtester bt(cfg, 128);
        std::mt19937_64 rng(0x1234abcdULL);
        std::uint64_t ts = 1000;
        for (int i = 0; i < 3000; ++i) {
            ts += 1 + rng() % 10;
            const auto type = static_cast<EventType>(rng() % 4);  // Add/Modify/Cancel/Trade
            const auto id = static_cast<OrderId>(1 + rng() % 100);
            const auto side = rng() % 2 == 0 ? Side::Buy : Side::Sell;
            const auto price = static_cast<Price>(90 + rng() % 21);
            const auto qty = static_cast<Quantity>(1 + rng() % 20);
            bt.on_event(sym_event(5, type, id, side, price, qty, ts));
        }
        return bt.finish();
    };
    const auto a = run();
    const auto b = run();
    CHECK(a.checksum == b.checksum);
    CHECK(a.position == b.position && a.cash_ticks == b.cash_ticks && a.trades == b.trades);
}

// --- M4: hardware cycle timing and thread affinity ---

void test_hardware_timing() {
    const auto start = cycle_now();
    volatile std::uint64_t sink = 0;
    for (int i = 0; i < 200'000; ++i) sink += static_cast<std::uint64_t>(i);
    (void)sink;
    const auto end = cycle_now();
    CHECK(end >= start);  // the counter is non-decreasing
    const double ns_per_cycle = calibrate_ns_per_cycle();
    CHECK(ns_per_cycle > 0.0);
}

void test_affinity_api() {
    const bool supported = affinity_supported();
    const bool pinned = pin_current_thread_to_core(0);
    if (!supported) CHECK(!pinned);  // unsupported platforms must report false, never crash
}

}  // namespace

int main() {
    try {
        test_order_book_operations();
        test_rejections_and_capacity();
        test_csv_is_deterministic();
        test_differential_after_every_event();
        test_randomized_differential();
        test_crossed_book_is_retained();
        test_checksum_observes_state_changes();
        test_matching_limit();
        test_matching_partial_and_rest();
        test_matching_time_in_force();
        test_matching_price_time_priority();
        test_matching_randomized_differential();
        test_reduce_and_replace();
        test_itch_round_trip();
        test_itch_malformed_and_skipped();
        test_mold_udp64_basic();
        test_mold_udp64_gap();
        test_mold_udp64_overlap();
        test_mold_udp64_retransmit_recovery();
        test_mold_udp64_retransmit_partial();
        test_itch_streaming_file_equivalence();
        test_soupbintcp();
        test_soupbintcp_malformed();
        test_multi_symbol_routing();
        test_l1_features();
        test_backtest_handworked();
        test_backtest_latency_delays_fill();
        test_backtest_determinism();
        test_hardware_timing();
        test_affinity_api();
        test_spsc_concurrently();
        std::cout << "all tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
