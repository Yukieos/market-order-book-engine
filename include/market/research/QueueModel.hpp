#pragma once

#include "market/MarketDataEvent.hpp"
#include "market/OrderBook.hpp"

#include <algorithm>
#include <cstdint>
#include <unordered_set>

// Phase 4: MBO (order-by-order) queue-position model for a single hypothetical passive
// order (docs/research-platform.md §4.2-§4.3). It tracks how much displayed size is ahead
// of our order and fills us when real executions at our price consume that queue and reach
// our position. Because full ITCH carries order references, a cancel/delete of a *known
// ahead* order advances us (it is not ignored -- ignoring it would be an artificial floor);
// a cancel of an order that arrived behind us does not.
//
// Two bounds share one class via `multiplier`:
//   - displayed (multiplier = 1.0): follow the visible book exactly.
//   - conservative (multiplier > 1.0): inflate the initial ahead-quantity to stand in for
//     hidden / iceberg liquidity we cannot observe. The band between them is the honest
//     unobservable-liquidity / counterfactual uncertainty, per the design doc.
//
// Counterfactual assumptions (small order, no market impact, historical events unchanged,
// fill *opportunity* not executable live PnL) are documented at the call sites and in the
// design doc; this class implements the mechanics only. It is fed each real event together
// with the book state *before* that event is applied, so it can look up the referenced
// order's price/side/quantity.
namespace market::research {

class QueueModel {
public:
    QueueModel(Side side, Price price, Quantity size, Quantity displayed_ahead,
               double multiplier, std::unordered_set<OrderId> cohort,
               std::uint64_t arrival_ns) noexcept
        : side_(side),
          price_(price),
          size_(size),
          ahead_(static_cast<double>(displayed_ahead) * multiplier),
          cohort_(std::move(cohort)),
          arrival_ns_(arrival_ns) {}

    // React to one real event, using the book state before the event is applied.
    void on_event(const MarketDataEvent& event, const OrderBook& book_before) noexcept {
        if (filled_ >= size_) return;
        switch (event.type) {
            case EventType::Trade: {  // execution at some price: consume ahead, overflow fills us
                if (!references_our_level(event.order_id, book_before)) return;
                const double q = static_cast<double>(event.quantity);
                const double consumed = std::min(q, ahead_);
                ahead_ -= consumed;
                const double overflow = q - consumed;
                if (overflow > 0.0) {
                    const auto room = static_cast<double>(size_ - filled_);
                    filled_ += static_cast<Quantity>(std::min(overflow, room));
                }
                return;
            }
            case EventType::Reduce: {  // partial cancel (X): only helps if ahead of us
                if (!references_our_level(event.order_id, book_before)) return;
                if (cohort_.count(event.order_id) != 0) {
                    ahead_ = std::max(0.0, ahead_ - static_cast<double>(event.quantity));
                }
                return;
            }
            case EventType::Cancel: {  // full delete (D): only helps if ahead of us
                if (const auto order = order_at_our_level(event.order_id, book_before)) {
                    if (cohort_.count(event.order_id) != 0) {
                        ahead_ = std::max(0.0, ahead_ - static_cast<double>(order->quantity));
                    }
                }
                return;
            }
            case EventType::Replace: {  // U: the original leaves our price if it was ahead
                if (const auto order = order_at_our_level(event.order_id, book_before)) {
                    if (cohort_.count(event.order_id) != 0) {
                        ahead_ = std::max(0.0, ahead_ - static_cast<double>(order->quantity));
                    }
                }
                return;
            }
            case EventType::Add:
            case EventType::Modify:
                return;  // adds queue behind us; Modify is not produced by ITCH
        }
    }

    [[nodiscard]] bool active() const noexcept { return filled_ < size_; }
    [[nodiscard]] bool fully_filled() const noexcept { return filled_ >= size_; }
    [[nodiscard]] Quantity filled() const noexcept { return filled_; }
    [[nodiscard]] Quantity size() const noexcept { return size_; }
    [[nodiscard]] double ahead() const noexcept { return ahead_; }
    [[nodiscard]] Price price() const noexcept { return price_; }
    [[nodiscard]] Side side() const noexcept { return side_; }

private:
    [[nodiscard]] std::optional<OrderView> order_at_our_level(
        OrderId id, const OrderBook& book) const noexcept {
        const auto order = book.find_order(id);
        if (order && order->side == side_ && order->price_ticks == price_) return order;
        return std::nullopt;
    }

    [[nodiscard]] bool references_our_level(OrderId id, const OrderBook& book) const noexcept {
        return order_at_our_level(id, book).has_value();
    }

    Side side_;
    Price price_;
    Quantity size_;
    double ahead_;
    Quantity filled_{0};
    std::unordered_set<OrderId> cohort_;
    std::uint64_t arrival_ns_;
};

}  // namespace market::research
