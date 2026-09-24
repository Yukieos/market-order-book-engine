#include "market/research/Backtest.hpp"

#include "market/StateChecksum.hpp"

#include <bit>
#include <cstdlib>

namespace market::research {

Backtester::Backtester(BacktestConfig config, std::size_t capacity)
    : config_(config),
      strategy_{config.enter_threshold, config.exit_threshold},
      book_(capacity) {}

void Backtester::execute_crossing(Side side, Quantity quantity, std::uint64_t now_ns) {
    // Frictionless crossing: fill the whole (small) size at the opposite touch, no queue,
    // no walking the book, no impact. This is the attribution ladder's upper bound.
    const auto opposite = side == Side::Buy ? book_.best_ask() : book_.best_bid();
    if (!opposite) {
        ++unfilled_;
        return;
    }
    const Price price = opposite->price_ticks;
    const auto signed_qty = static_cast<std::int64_t>(quantity);
    if (side == Side::Buy) {
        position_ += signed_qty;
        cash_ticks_ -= price * signed_qty;
    } else {
        position_ -= signed_qty;
        cash_ticks_ += price * signed_qty;
    }
    fills_.push_back({now_ns, side, price, quantity});
    ++trades_;

    // Order-dependent running checksum over the fill sequence (integer -> reproducible).
    std::uint64_t h = checksum_mix(now_ns);
    h ^= std::rotl(checksum_mix(static_cast<std::uint64_t>(price)), 11);
    h ^= std::rotl(checksum_mix(quantity), 23);
    h ^= side == Side::Buy ? 0x6a09e667f3bcc909ULL : 0xbb67ae8584caa73bULL;
    checksum_ = checksum_mix(checksum_ ^ h);
}

void Backtester::release_arrived(std::uint64_t now_ns) {
    // Orders that arrived before this event execute against the current (pre-event) book.
    while (!pending_.empty() && pending_.front().arrival_ns <= now_ns) {
        const Pending order = pending_.front();
        pending_.pop_front();
        execute_crossing(order.side, order.quantity, order.arrival_ns);
    }
}

void Backtester::on_event(const MarketDataEvent& event) {
    if (event.symbol != config_.locate) return;

    // 1. Release any orders that have arrived, against the pre-event book state.
    release_arrived(event.exchange_timestamp_ns);

    // 2. Apply the event.
    book_.process_event(event);
    last_ts_ = event.exchange_timestamp_ns;

    // 3. Post-event feature; act only on an L1 change (the sampling clock).
    const L1Row l1 = l1_row(event.sequence, event.exchange_timestamp_ns, book_);
    if (!l1.two_sided) return;
    if (have_l1_ && !l1_changed(prev_l1_, l1)) return;
    prev_l1_ = l1;
    const bool first = !have_l1_;
    have_l1_ = true;
    ++l1_updates_;

    const Signal current =
        committed_units_ > 0 ? Signal::Long : (committed_units_ < 0 ? Signal::Short : Signal::Flat);
    const Signal target = strategy_.target(l1, current);
    const std::int64_t target_units =
        static_cast<std::int64_t>(target) * static_cast<std::int64_t>(config_.order_size);
    if (first && target == Signal::Flat) return;
    if (target_units == committed_units_) return;

    const std::int64_t delta = target_units - committed_units_;
    committed_units_ = target_units;
    const Side side = delta > 0 ? Side::Buy : Side::Sell;
    const auto quantity = static_cast<Quantity>(std::llabs(delta));

    if (config_.latency_ns == 0) {
        // Frictionless-immediate: fill at the post-event touch, this instant.
        execute_crossing(side, quantity, event.exchange_timestamp_ns);
    } else {
        pending_.push_back({event.exchange_timestamp_ns + config_.latency_ns, side, quantity});
    }
}

BacktestResult Backtester::finish() {
    // Release everything still in flight against the final book.
    while (!pending_.empty()) {
        const Pending order = pending_.front();
        pending_.pop_front();
        execute_crossing(order.side, order.quantity, order.arrival_ns);
    }

    // Flatten any residual position at session end so PnL is fully realized (standard for
    // a backtest); a leftover mark-to-market position otherwise dominates the number.
    if (position_ != 0) {
        const Side side = position_ > 0 ? Side::Sell : Side::Buy;
        execute_crossing(side, static_cast<Quantity>(std::llabs(position_)), last_ts_);
    }

    BacktestResult result;
    result.position = position_;
    result.cash_ticks = cash_ticks_;
    result.trades = trades_;
    result.unfilled = unfilled_;
    result.l1_updates = l1_updates_;
    if (have_l1_) {
        result.final_pnl_2x = 2 * cash_ticks_ + position_ * (prev_l1_.bid_px + prev_l1_.ask_px);
    }
    // Fold final position/cash into the checksum so state, not just fills, is covered.
    std::uint64_t h = checksum_mix(static_cast<std::uint64_t>(position_));
    h ^= std::rotl(checksum_mix(static_cast<std::uint64_t>(cash_ticks_)), 29);
    result.checksum = checksum_mix(checksum_ ^ h);
    return result;
}

}  // namespace market::research
