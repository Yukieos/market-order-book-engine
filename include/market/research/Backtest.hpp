#pragma once

#include "market/MarketDataEvent.hpp"
#include "market/OrderBook.hpp"
#include "market/research/Features.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <vector>

// Phase 3: the event-driven backtest loop and time model (docs/research-platform.md §2,
// §7, Phase 3). Single pass over one symbol's events; the strategy sees only
// past-and-current book state, its decision enters the sim at decision_time + latency,
// and a crossing order fills at the touch on arrival (the frictionless upper bound — the
// MBO queue model and fees/markout arrive in Phases 4-5). The ledger is integer / fixed
// point so a run is byte-reproducible (float analytics live in Python).
namespace market::research {

enum class Signal : int { Short = -1, Flat = 0, Long = 1 };

// Leak-free imbalance strategy: top-of-book imbalance -> a target signal, with
// hysteresis (enter above `enter_threshold`, flatten below `exit_threshold`, hold in
// between). Uses only the current L1 row (present-and-past state).
struct ImbalanceStrategy {
    double enter_threshold{0.30};
    double exit_threshold{0.10};

    [[nodiscard]] Signal target(const L1Row& l1, Signal current) const noexcept {
        const double total = static_cast<double>(l1.bid_sz) + static_cast<double>(l1.ask_sz);
        if (total <= 0.0) return Signal::Flat;
        const double imbalance =
            (static_cast<double>(l1.bid_sz) - static_cast<double>(l1.ask_sz)) / total;
        if (imbalance >= enter_threshold) return Signal::Long;
        if (imbalance <= -enter_threshold) return Signal::Short;
        if (imbalance <= exit_threshold && imbalance >= -exit_threshold) return Signal::Flat;
        return current;  // hysteresis band: hold
    }
};

struct Fill {
    std::uint64_t timestamp_ns{};
    Side side{};       // taker side
    Price price{};     // touch price at arrival
    Quantity quantity{};
};

struct BacktestConfig {
    SymbolId locate{};
    std::uint64_t latency_ns{0};  // decision_time -> arrival_time
    Quantity order_size{1};
    double enter_threshold{0.30};
    double exit_threshold{0.10};
};

struct BacktestResult {
    std::int64_t position{0};        // final signed units
    std::int64_t cash_ticks{0};      // final integer cash (sum of -/+ price*qty)
    std::int64_t final_pnl_2x{0};    // 2*PnL in ticks at the last two-sided mid (integer)
    std::uint64_t trades{0};
    std::uint64_t unfilled{0};       // crossing orders that found no opposite liquidity
    std::size_t l1_updates{0};
    std::uint64_t checksum{0};       // deterministic over the fill sequence + final state
};

class Backtester {
public:
    Backtester(BacktestConfig config, std::size_t capacity);

    // Feed events in stream order (all symbols; non-target symbols are ignored).
    void on_event(const MarketDataEvent& event);

    // Release any still-pending orders against the final book and return the result.
    BacktestResult finish();

    [[nodiscard]] const std::vector<Fill>& fills() const noexcept { return fills_; }

private:
    struct Pending {
        std::uint64_t arrival_ns{};
        Side side{};
        Quantity quantity{};
    };

    void release_arrived(std::uint64_t now_ns);
    void execute_crossing(Side side, Quantity quantity, std::uint64_t now_ns);

    BacktestConfig config_;
    ImbalanceStrategy strategy_;
    OrderBook book_;
    std::deque<Pending> pending_;
    std::vector<Fill> fills_;

    L1Row prev_l1_{};
    bool have_l1_{false};
    std::int64_t committed_units_{0};  // filled + in-flight target position
    std::int64_t position_{0};
    std::int64_t cash_ticks_{0};
    std::uint64_t trades_{0};
    std::uint64_t unfilled_{0};
    std::size_t l1_updates_{0};
    std::uint64_t checksum_{0};
    std::uint64_t last_ts_{0};
};

}  // namespace market::research
