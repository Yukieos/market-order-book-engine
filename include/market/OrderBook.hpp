#pragma once

#include "market/MarketDataEvent.hpp"
#include "market/OrderTypes.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace market {

enum class ProcessResult : std::uint8_t {
    Applied,
    DuplicateOrderId,
    OrderNotFound,
    InvalidQuantity,
    CapacityExhausted
};

struct OrderView {
    OrderId id{};
    Side side{};
    Price price_ticks{};
    Quantity quantity{};
    std::uint64_t priority{};
    friend bool operator==(const OrderView&, const OrderView&) = default;
};

struct LevelView {
    Side side{};
    Price price_ticks{};
    Quantity total_quantity{};
    std::size_t order_count{};
    friend bool operator==(const LevelView&, const LevelView&) = default;
};

class OrderBook {
public:
    explicit OrderBook(std::size_t max_orders);

    // Apply mode: replay an exchange-decided event (CSV / ITCH). No matching.
    ProcessResult process_event(const MarketDataEvent& event) noexcept;

    // Match mode: match an incoming order against the resting book by price-time
    // priority, emitting fills to `sink`, then rest/kill the remainder per TIF.
    // noexcept and allocation-free; see ARCHITECTURE.md section 3.
    SubmitResult submit(const OrderRequest& request, const FillSink& sink) noexcept;

    [[nodiscard]] std::optional<OrderView> find_order(OrderId id) const noexcept;
    [[nodiscard]] std::optional<LevelView> best_bid() const noexcept;
    [[nodiscard]] std::optional<LevelView> best_ask() const noexcept;
    [[nodiscard]] std::vector<OrderView> orders_in_priority() const;
    [[nodiscard]] std::vector<LevelView> levels() const;
    [[nodiscard]] std::uint64_t state_checksum() const noexcept;
    [[nodiscard]] std::size_t order_count() const noexcept { return active_orders_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return orders_.size(); }

private:
    static constexpr std::uint32_t npos = UINT32_MAX;

    struct Order {
        OrderId id{};
        Price price{};
        Quantity quantity{};
        std::uint64_t priority{};
        std::uint32_t level{npos};
        std::uint32_t prev{npos};
        std::uint32_t next{npos};
        std::uint32_t next_free{npos};
        Side side{};
        bool active{false};
    };

    struct Level {
        Price price{};
        Quantity total_quantity{};
        std::size_t order_count{};
        std::uint32_t head{npos};
        std::uint32_t tail{npos};
        std::uint32_t next_free{npos};
        std::uint32_t heap_pos{npos};  // position in its side's price-ladder heap
        Side side{};
        bool active{false};
    };

    enum class SlotState : std::uint8_t { Empty, Occupied, Tombstone };
    struct OrderSlot { OrderId key{}; std::uint32_t value{npos}; SlotState state{SlotState::Empty}; };
    struct LevelSlot { Price price{}; std::uint32_t value{npos}; Side side{}; SlotState state{SlotState::Empty}; };

    ProcessResult add(const MarketDataEvent& event) noexcept;
    ProcessResult modify(const MarketDataEvent& event) noexcept;
    ProcessResult cancel(OrderId id) noexcept;
    ProcessResult trade(OrderId id, Quantity quantity) noexcept;
    ProcessResult replace(const MarketDataEvent& event) noexcept;
    ProcessResult rest_order(OrderId id, Side side, Price price, Quantity quantity) noexcept;
    std::uint32_t find_order_index(OrderId id) const noexcept;
    std::uint32_t find_level_index(Side side, Price price) const noexcept;
    std::uint32_t acquire_level(Side side, Price price) noexcept;
    void release_level(std::uint32_t index) noexcept;
    void link_tail(std::uint32_t order_index, std::uint32_t level_index) noexcept;
    void unlink(std::uint32_t order_index) noexcept;

    // Price ladder: per-side indexed binary heap of active level slots keyed by
    // price (max-heap for bids, min-heap for asks). O(1) best price, O(log L)
    // push/remove. Maintained only in acquire_level()/release_level().
    [[nodiscard]] bool ladder_before(Side side, std::uint32_t a, std::uint32_t b) const noexcept;
    void ladder_push(Side side, std::uint32_t level_index) noexcept;
    void ladder_remove(Side side, std::uint32_t level_index) noexcept;
    void ladder_sift_up(Side side, std::size_t pos) noexcept;
    void ladder_sift_down(Side side, std::size_t pos) noexcept;
    [[nodiscard]] std::uint32_t ladder_top(Side side) const noexcept;
    [[nodiscard]] bool crosses(const OrderRequest& request, Price level_price) const noexcept;
    [[nodiscard]] Quantity fillable_quantity(const OrderRequest& request) const noexcept;
    void free_order_slot(std::uint32_t order_index) noexcept;
    bool insert_order_lookup(OrderId id, std::uint32_t value) noexcept;
    void erase_order_lookup(OrderId id) noexcept;
    bool insert_level_lookup(Side side, Price price, std::uint32_t value) noexcept;
    void erase_level_lookup(Side side, Price price) noexcept;

    std::vector<Order> orders_;
    std::vector<Level> levels_;
    std::vector<OrderSlot> order_lookup_;
    std::vector<LevelSlot> level_lookup_;
    std::vector<std::uint32_t> bid_heap_;  // level indices, max-heap by price
    std::vector<std::uint32_t> ask_heap_;  // level indices, min-heap by price
    std::size_t bid_heap_size_{0};
    std::size_t ask_heap_size_{0};
    std::uint32_t free_order_{npos};
    std::uint32_t free_level_{npos};
    std::size_t active_orders_{0};
    std::uint64_t next_priority_{0};
    std::uint64_t fill_sequence_{0};
};

}  // namespace market
