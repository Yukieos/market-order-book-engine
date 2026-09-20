#include "market/OrderBook.hpp"
#include "market/StateChecksum.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace market {
namespace {

std::size_t next_power_of_two(std::size_t value) {
    std::size_t result = 1;
    while (result < value) result <<= 1U;
    return result;
}

std::size_t mix(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return static_cast<std::size_t>(value);
}

}  // namespace

OrderBook::OrderBook(std::size_t max_orders)
    : orders_(max_orders),
      levels_(max_orders),
      order_lookup_(next_power_of_two(max_orders * 2)),
      level_lookup_(next_power_of_two(max_orders * 2)),
      bid_heap_(max_orders),
      ask_heap_(max_orders) {
    if (max_orders == 0 || max_orders > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("max_orders must fit in uint32_t and be non-zero");
    }
    for (std::size_t i = 0; i < max_orders; ++i) {
        orders_[i].next_free = i + 1 < max_orders ? static_cast<std::uint32_t>(i + 1) : npos;
        levels_[i].next_free = i + 1 < max_orders ? static_cast<std::uint32_t>(i + 1) : npos;
    }
    free_order_ = 0;
    free_level_ = 0;
}

ProcessResult OrderBook::process_event(const MarketDataEvent& event) noexcept {
    switch (event.type) {
        case EventType::Add: return add(event);
        case EventType::Modify: return modify(event);
        case EventType::Cancel: return cancel(event.order_id);
        // Trade (execution) and Reduce (partial cancel) apply the same book
        // mutation; they differ only in downstream analytics labeling.
        case EventType::Trade: return trade(event.order_id, event.quantity);
        case EventType::Reduce: return trade(event.order_id, event.quantity);
        case EventType::Replace: return replace(event);
    }
    return ProcessResult::OrderNotFound;
}

ProcessResult OrderBook::add(const MarketDataEvent& event) noexcept {
    return rest_order(event.order_id, event.side, event.price_ticks, event.quantity);
}

ProcessResult OrderBook::modify(const MarketDataEvent& event) noexcept {
    if (event.quantity == 0) return ProcessResult::InvalidQuantity;
    const auto index = find_order_index(event.order_id);
    if (index == npos) return ProcessResult::OrderNotFound;
    auto& order = orders_[index];
    if (order.side == event.side && order.price == event.price_ticks) {
        auto& level = levels_[order.level];
        level.total_quantity -= order.quantity;
        level.total_quantity += event.quantity;
        order.quantity = event.quantity;
        return ProcessResult::Applied;
    }

    auto target = find_level_index(event.side, event.price_ticks);
    if (target == npos && free_level_ == npos && levels_[order.level].order_count > 1) {
        return ProcessResult::CapacityExhausted;
    }
    unlink(index);
    target = acquire_level(event.side, event.price_ticks);
    if (target == npos) return ProcessResult::CapacityExhausted;  // unreachable after precheck
    order.side = event.side;
    order.price = event.price_ticks;
    order.quantity = event.quantity;
    order.priority = next_priority_++;
    order.level = target;
    link_tail(index, target);
    return ProcessResult::Applied;
}

ProcessResult OrderBook::cancel(OrderId id) noexcept {
    const auto index = find_order_index(id);
    if (index == npos) return ProcessResult::OrderNotFound;
    unlink(index);
    erase_order_lookup(id);
    free_order_slot(index);
    --active_orders_;
    return ProcessResult::Applied;
}

void OrderBook::free_order_slot(std::uint32_t order_index) noexcept {
    auto& order = orders_[order_index];
    order.active = false;
    order.next_free = free_order_;
    free_order_ = order_index;
}

ProcessResult OrderBook::replace(const MarketDataEvent& event) noexcept {
    if (event.quantity == 0) return ProcessResult::InvalidQuantity;
    const auto index = find_order_index(event.order_id);
    if (index == npos) return ProcessResult::OrderNotFound;
    const Side side = orders_[index].side;  // Replace inherits the original side.
    if (event.new_order_id != event.order_id &&
        find_order_index(event.new_order_id) != npos) {
        return ProcessResult::DuplicateOrderId;
    }
    cancel(event.order_id);  // frees an order slot for the replacement
    return rest_order(event.new_order_id, side, event.price_ticks, event.quantity);
}

// Insert a fresh resting order; the shared insertion path for add() and for the
// remainder of a matched limit order.
ProcessResult OrderBook::rest_order(OrderId id, Side side, Price price,
                                    Quantity quantity) noexcept {
    if (quantity == 0) return ProcessResult::InvalidQuantity;
    if (find_order_index(id) != npos) return ProcessResult::DuplicateOrderId;
    if (free_order_ == npos) return ProcessResult::CapacityExhausted;
    const auto level_index = acquire_level(side, price);
    if (level_index == npos) return ProcessResult::CapacityExhausted;
    const auto order_index = free_order_;
    free_order_ = orders_[order_index].next_free;
    auto& order = orders_[order_index];
    order = Order{.id = id,
                  .price = price,
                  .quantity = quantity,
                  .priority = next_priority_++,
                  .level = level_index,
                  .side = side,
                  .active = true};
    link_tail(order_index, level_index);
    if (!insert_order_lookup(id, order_index)) {
        unlink(order_index);
        free_order_slot(order_index);
        return ProcessResult::CapacityExhausted;
    }
    ++active_orders_;
    return ProcessResult::Applied;
}

ProcessResult OrderBook::trade(OrderId id, Quantity quantity) noexcept {
    if (quantity == 0) return ProcessResult::InvalidQuantity;
    const auto index = find_order_index(id);
    if (index == npos) return ProcessResult::OrderNotFound;
    auto& order = orders_[index];
    if (quantity > order.quantity) return ProcessResult::InvalidQuantity;
    if (quantity == order.quantity) return cancel(id);
    order.quantity -= quantity;
    levels_[order.level].total_quantity -= quantity;
    return ProcessResult::Applied;
}

std::uint32_t OrderBook::find_order_index(OrderId id) const noexcept {
    const auto mask = order_lookup_.size() - 1;
    auto slot = mix(id) & mask;
    for (std::size_t probe = 0; probe < order_lookup_.size(); ++probe) {
        const auto& entry = order_lookup_[slot];
        if (entry.state == SlotState::Empty) return npos;
        if (entry.state == SlotState::Occupied && entry.key == id) return entry.value;
        slot = (slot + 1) & mask;
    }
    return npos;
}

std::uint32_t OrderBook::find_level_index(Side side, Price price) const noexcept {
    const auto mask = level_lookup_.size() - 1;
    auto slot = mix(static_cast<std::uint64_t>(price) ^ (side == Side::Buy ? 0ULL : 0x9e3779b97f4a7c15ULL)) & mask;
    for (std::size_t probe = 0; probe < level_lookup_.size(); ++probe) {
        const auto& entry = level_lookup_[slot];
        if (entry.state == SlotState::Empty) return npos;
        if (entry.state == SlotState::Occupied && entry.side == side && entry.price == price) return entry.value;
        slot = (slot + 1) & mask;
    }
    return npos;
}

std::uint32_t OrderBook::acquire_level(Side side, Price price) noexcept {
    if (const auto existing = find_level_index(side, price); existing != npos) return existing;
    if (free_level_ == npos) return npos;
    const auto index = free_level_;
    free_level_ = levels_[index].next_free;
    levels_[index] = Level{.price = price, .side = side, .active = true};
    if (!insert_level_lookup(side, price, index)) {
        levels_[index].active = false;
        levels_[index].next_free = free_level_;
        free_level_ = index;
        return npos;
    }
    ladder_push(side, index);
    return index;
}

void OrderBook::release_level(std::uint32_t index) noexcept {
    auto& level = levels_[index];
    ladder_remove(level.side, index);
    erase_level_lookup(level.side, level.price);
    level.active = false;
    level.next_free = free_level_;
    free_level_ = index;
}

bool OrderBook::ladder_before(Side side, std::uint32_t a, std::uint32_t b) const noexcept {
    // "before" == higher matching priority == closer to the top of the book.
    return side == Side::Buy ? levels_[a].price > levels_[b].price
                             : levels_[a].price < levels_[b].price;
}

void OrderBook::ladder_sift_up(Side side, std::size_t pos) noexcept {
    auto& heap = side == Side::Buy ? bid_heap_ : ask_heap_;
    while (pos > 0) {
        const std::size_t parent = (pos - 1) / 2;
        if (!ladder_before(side, heap[pos], heap[parent])) break;
        std::swap(heap[pos], heap[parent]);
        levels_[heap[pos]].heap_pos = static_cast<std::uint32_t>(pos);
        levels_[heap[parent]].heap_pos = static_cast<std::uint32_t>(parent);
        pos = parent;
    }
}

void OrderBook::ladder_sift_down(Side side, std::size_t pos) noexcept {
    auto& heap = side == Side::Buy ? bid_heap_ : ask_heap_;
    const std::size_t size = side == Side::Buy ? bid_heap_size_ : ask_heap_size_;
    for (;;) {
        const std::size_t left = 2 * pos + 1;
        const std::size_t right = 2 * pos + 2;
        std::size_t best = pos;
        if (left < size && ladder_before(side, heap[left], heap[best])) best = left;
        if (right < size && ladder_before(side, heap[right], heap[best])) best = right;
        if (best == pos) break;
        std::swap(heap[pos], heap[best]);
        levels_[heap[pos]].heap_pos = static_cast<std::uint32_t>(pos);
        levels_[heap[best]].heap_pos = static_cast<std::uint32_t>(best);
        pos = best;
    }
}

void OrderBook::ladder_push(Side side, std::uint32_t level_index) noexcept {
    auto& heap = side == Side::Buy ? bid_heap_ : ask_heap_;
    auto& size = side == Side::Buy ? bid_heap_size_ : ask_heap_size_;
    const std::size_t pos = size++;
    heap[pos] = level_index;
    levels_[level_index].heap_pos = static_cast<std::uint32_t>(pos);
    ladder_sift_up(side, pos);
}

void OrderBook::ladder_remove(Side side, std::uint32_t level_index) noexcept {
    auto& heap = side == Side::Buy ? bid_heap_ : ask_heap_;
    auto& size = side == Side::Buy ? bid_heap_size_ : ask_heap_size_;
    const std::size_t pos = levels_[level_index].heap_pos;
    --size;
    if (pos != size) {
        heap[pos] = heap[size];
        levels_[heap[pos]].heap_pos = static_cast<std::uint32_t>(pos);
        ladder_sift_down(side, pos);
        ladder_sift_up(side, pos);
    }
    levels_[level_index].heap_pos = npos;
}

std::uint32_t OrderBook::ladder_top(Side side) const noexcept {
    const std::size_t size = side == Side::Buy ? bid_heap_size_ : ask_heap_size_;
    if (size == 0) return npos;
    return side == Side::Buy ? bid_heap_[0] : ask_heap_[0];
}

void OrderBook::link_tail(std::uint32_t order_index, std::uint32_t level_index) noexcept {
    auto& level = levels_[level_index];
    auto& order = orders_[order_index];
    order.prev = level.tail;
    order.next = npos;
    if (level.tail != npos) orders_[level.tail].next = order_index;
    else level.head = order_index;
    level.tail = order_index;
    level.total_quantity += order.quantity;
    ++level.order_count;
}

void OrderBook::unlink(std::uint32_t order_index) noexcept {
    auto& order = orders_[order_index];
    auto& level = levels_[order.level];
    if (order.prev != npos) orders_[order.prev].next = order.next;
    else level.head = order.next;
    if (order.next != npos) orders_[order.next].prev = order.prev;
    else level.tail = order.prev;
    level.total_quantity -= order.quantity;
    --level.order_count;
    order.prev = order.next = npos;
    if (level.order_count == 0) release_level(order.level);
}

bool OrderBook::insert_order_lookup(OrderId id, std::uint32_t value) noexcept {
    const auto mask = order_lookup_.size() - 1;
    auto slot = mix(id) & mask;
    std::size_t tombstone = order_lookup_.size();
    for (std::size_t probe = 0; probe < order_lookup_.size(); ++probe) {
        auto& entry = order_lookup_[slot];
        if (entry.state == SlotState::Tombstone && tombstone == order_lookup_.size()) tombstone = slot;
        if (entry.state == SlotState::Empty) {
            auto& destination = order_lookup_[tombstone == order_lookup_.size() ? slot : tombstone];
            destination = OrderSlot{.key = id, .value = value, .state = SlotState::Occupied};
            return true;
        }
        slot = (slot + 1) & mask;
    }
    if (tombstone != order_lookup_.size()) {
        order_lookup_[tombstone] = OrderSlot{.key = id, .value = value, .state = SlotState::Occupied};
        return true;
    }
    return false;
}

void OrderBook::erase_order_lookup(OrderId id) noexcept {
    const auto mask = order_lookup_.size() - 1;
    auto slot = mix(id) & mask;
    for (std::size_t probe = 0; probe < order_lookup_.size(); ++probe) {
        auto& entry = order_lookup_[slot];
        if (entry.state == SlotState::Empty) return;
        if (entry.state == SlotState::Occupied && entry.key == id) {
            entry.state = SlotState::Tombstone;
            return;
        }
        slot = (slot + 1) & mask;
    }
}

bool OrderBook::insert_level_lookup(Side side, Price price, std::uint32_t value) noexcept {
    const auto mask = level_lookup_.size() - 1;
    auto slot = mix(static_cast<std::uint64_t>(price) ^ (side == Side::Buy ? 0ULL : 0x9e3779b97f4a7c15ULL)) & mask;
    std::size_t tombstone = level_lookup_.size();
    for (std::size_t probe = 0; probe < level_lookup_.size(); ++probe) {
        auto& entry = level_lookup_[slot];
        if (entry.state == SlotState::Tombstone && tombstone == level_lookup_.size()) tombstone = slot;
        if (entry.state == SlotState::Empty) {
            auto& destination = level_lookup_[tombstone == level_lookup_.size() ? slot : tombstone];
            destination = LevelSlot{.price = price, .value = value, .side = side, .state = SlotState::Occupied};
            return true;
        }
        slot = (slot + 1) & mask;
    }
    return false;
}

void OrderBook::erase_level_lookup(Side side, Price price) noexcept {
    const auto mask = level_lookup_.size() - 1;
    auto slot = mix(static_cast<std::uint64_t>(price) ^ (side == Side::Buy ? 0ULL : 0x9e3779b97f4a7c15ULL)) & mask;
    for (std::size_t probe = 0; probe < level_lookup_.size(); ++probe) {
        auto& entry = level_lookup_[slot];
        if (entry.state == SlotState::Empty) return;
        if (entry.state == SlotState::Occupied && entry.side == side && entry.price == price) {
            entry.state = SlotState::Tombstone;
            return;
        }
        slot = (slot + 1) & mask;
    }
}

std::optional<OrderView> OrderBook::find_order(OrderId id) const noexcept {
    const auto index = find_order_index(id);
    if (index == npos) return std::nullopt;
    const auto& order = orders_[index];
    return OrderView{order.id, order.side, order.price, order.quantity, order.priority};
}

std::optional<LevelView> OrderBook::best_bid() const noexcept {
    const auto top = ladder_top(Side::Buy);
    if (top == npos) return std::nullopt;
    const auto& level = levels_[top];
    return LevelView{level.side, level.price, level.total_quantity, level.order_count};
}

std::optional<LevelView> OrderBook::best_ask() const noexcept {
    const auto top = ladder_top(Side::Sell);
    if (top == npos) return std::nullopt;
    const auto& level = levels_[top];
    return LevelView{level.side, level.price, level.total_quantity, level.order_count};
}

bool OrderBook::crosses(const OrderRequest& request, Price level_price) const noexcept {
    if (request.order_type == OrderType::Market) return true;
    return request.side == Side::Buy ? request.price_ticks >= level_price
                                     : request.price_ticks <= level_price;
}

Quantity OrderBook::fillable_quantity(const OrderRequest& request) const noexcept {
    const Side opposite = request.side == Side::Buy ? Side::Sell : Side::Buy;
    const auto& heap = opposite == Side::Buy ? bid_heap_ : ask_heap_;
    const std::size_t size = opposite == Side::Buy ? bid_heap_size_ : ask_heap_size_;
    Quantity available = 0;
    for (std::size_t i = 0; i < size; ++i) {
        const auto& level = levels_[heap[i]];
        if (crosses(request, level.price)) available += level.total_quantity;
    }
    return available;
}

SubmitResult OrderBook::submit(const OrderRequest& request, const FillSink& sink) noexcept {
    if (request.type == RequestType::Cancel) {
        return cancel(request.id) == ProcessResult::Applied ? SubmitResult::Canceled
                                                            : SubmitResult::RejectedInvalid;
    }
    if (request.quantity == 0) return SubmitResult::RejectedInvalid;
    const bool may_rest =
        request.order_type == OrderType::Limit && request.tif == TimeInForce::GTC;
    // A resting order needs a unique id; reject up front so we never emit fills
    // and then discover a collision when trying to rest the remainder.
    if (may_rest && find_order_index(request.id) != npos) {
        return SubmitResult::RejectedDuplicate;
    }
    if (request.tif == TimeInForce::FOK && fillable_quantity(request) < request.quantity) {
        return SubmitResult::RejectedFOK;
    }

    const Side opposite = request.side == Side::Buy ? Side::Sell : Side::Buy;
    Quantity remaining = request.quantity;
    while (remaining > 0) {
        const auto level_index = ladder_top(opposite);
        if (level_index == npos) break;
        auto& level = levels_[level_index];
        if (!crosses(request, level.price)) break;
        const Price trade_price = level.price;
        auto order_index = level.head;
        while (order_index != npos && remaining > 0) {
            auto& maker = orders_[order_index];
            const Quantity traded = std::min(remaining, maker.quantity);
            sink(Fill{.taker_id = request.id,
                      .maker_id = maker.id,
                      .taker_side = request.side,
                      .price_ticks = trade_price,
                      .quantity = traded,
                      .sequence = fill_sequence_++});
            remaining -= traded;
            const auto next_index = maker.next;
            if (traded == maker.quantity) {
                erase_order_lookup(maker.id);
                unlink(order_index);  // may release the level + pop the ladder
                free_order_slot(order_index);
                --active_orders_;
                order_index = next_index;
            } else {
                maker.quantity -= traded;
                level.total_quantity -= traded;  // remaining is now 0; loops exit
            }
        }
    }

    const Quantity filled = request.quantity - remaining;
    if (remaining == 0) return SubmitResult::FilledComplete;
    if (may_rest) {
        const auto rested = rest_order(request.id, request.side, request.price_ticks, remaining);
        if (rested == ProcessResult::CapacityExhausted) return SubmitResult::RejectedCapacity;
        return filled == 0 ? SubmitResult::RestedNoFill : SubmitResult::PartialFillRested;
    }
    return filled == 0 ? SubmitResult::NoFillKilled : SubmitResult::PartialFillKilled;
}

std::vector<LevelView> OrderBook::levels() const {
    std::vector<LevelView> result;
    result.reserve(active_orders_);
    for (const auto& level : levels_) {
        if (level.active) result.push_back({level.side, level.price, level.total_quantity, level.order_count});
    }
    std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
        if (left.side != right.side) return left.side == Side::Buy;
        return left.side == Side::Buy ? left.price_ticks > right.price_ticks : left.price_ticks < right.price_ticks;
    });
    return result;
}

std::vector<OrderView> OrderBook::orders_in_priority() const {
    std::vector<OrderView> result;
    result.reserve(active_orders_);
    const auto sorted_levels = levels();
    for (const auto& view : sorted_levels) {
        auto index = levels_[find_level_index(view.side, view.price_ticks)].head;
        while (index != npos) {
            const auto& order = orders_[index];
            result.push_back({order.id, order.side, order.price, order.quantity, order.priority});
            index = order.next;
        }
    }
    return result;
}

std::uint64_t OrderBook::state_checksum() const noexcept {
    auto result = checksum_mix(static_cast<std::uint64_t>(active_orders_));
    for (const auto& order : orders_) {
        if (order.active) {
            result ^= order_checksum({order.id, order.side, order.price, order.quantity, order.priority});
        }
    }
    return result;
}

}  // namespace market
