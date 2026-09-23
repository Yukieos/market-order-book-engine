#pragma once

#include "market/MarketDataEvent.hpp"
#include "market/OrderBook.hpp"

#include <cstdint>
#include <ostream>

// Phase 2 (signal-validity study): extract a top-of-book (L1) feature row from a book
// *after* an event is applied. Only past-and-current book state is read here; forward
// returns (the label) are computed downstream in Python from future rows, so the feature
// side is leakage-proof by construction (see docs/research-platform.md §2-§3).
//
// All fields are integer, so the emitted stream is deterministic and byte-reproducible.
// The imbalance ratio and forward returns are intentionally left to the Python analysis
// layer (float analytics live there, not in the deterministic C++ ledger).
namespace market::research {

struct L1Row {
    std::uint64_t sequence{};
    std::uint64_t timestamp_ns{};
    Price bid_px{};       // 0 when no bid
    Quantity bid_sz{};    // total size at best bid
    Price ask_px{};       // 0 when no ask
    Quantity ask_sz{};    // total size at best ask
    bool two_sided{};     // both sides present (an imbalance is defined)

    friend bool operator==(const L1Row&, const L1Row&) = default;
};

// Snapshot the post-event top of book. `book` must already have the event applied.
[[nodiscard]] inline L1Row l1_row(std::uint64_t sequence, std::uint64_t timestamp_ns,
                                  const OrderBook& book) noexcept {
    const auto bid = book.best_bid();
    const auto ask = book.best_ask();
    L1Row row{sequence, timestamp_ns, 0, 0, 0, 0, bid.has_value() && ask.has_value()};
    if (bid) {
        row.bid_px = bid->price_ticks;
        row.bid_sz = bid->total_quantity;
    }
    if (ask) {
        row.ask_px = ask->price_ticks;
        row.ask_sz = ask->total_quantity;
    }
    return row;
}

// True when the observable L1 state changed (the natural microstructure sampling clock).
[[nodiscard]] inline bool l1_changed(const L1Row& a, const L1Row& b) noexcept {
    return a.bid_px != b.bid_px || a.bid_sz != b.bid_sz || a.ask_px != b.ask_px ||
           a.ask_sz != b.ask_sz;
}

inline void write_l1_header(std::ostream& out) {
    out << "sequence,timestamp_ns,bid_px,bid_sz,ask_px,ask_sz,two_sided\n";
}

inline void write_l1_row(std::ostream& out, const L1Row& r) {
    out << r.sequence << ',' << r.timestamp_ns << ',' << r.bid_px << ',' << r.bid_sz << ','
        << r.ask_px << ',' << r.ask_sz << ',' << (r.two_sided ? 1 : 0) << '\n';
}

}  // namespace market::research
