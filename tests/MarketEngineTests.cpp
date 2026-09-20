#include "market/CSVReplayConnector.hpp"
#include "market/OrderBook.hpp"
#include "market/SpscQueue.hpp"
#include "market/StateChecksum.hpp"

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
        test_spsc_concurrently();
        std::cout << "all tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
