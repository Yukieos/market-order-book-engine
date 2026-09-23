#pragma once

#include "market/MarketDataEvent.hpp"
#include "market/OrderBook.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace market {

// Routes events to one OrderBook per symbol (ITCH stock_locate), so a multi-symbol
// feed reconstructs a real per-symbol book instead of collapsing every symbol into
// one (which makes best bid/ask a meaningless cross-symbol extremum).
//
// Books are created lazily on the first event for a symbol (a setup-time allocation,
// like connector construction); routing an event afterward is an O(1) indexed lookup
// with no allocation. Each per-symbol book has a fixed capacity, since a single
// symbol's peak resting-order count is far smaller than a whole feed's.
class MultiSymbolBook {
public:
    explicit MultiSymbolBook(std::size_t per_symbol_capacity,
                             std::size_t max_symbols = 1u << 16);

    // Routes to the symbol's book (creating it on first sight). Returns the book's
    // ProcessResult, or CapacityExhausted if the symbol id is out of range.
    ProcessResult process_event(const MarketDataEvent& event);

    [[nodiscard]] const OrderBook* book(SymbolId symbol) const noexcept;
    [[nodiscard]] std::optional<LevelView> best_bid(SymbolId symbol) const noexcept;
    [[nodiscard]] std::optional<LevelView> best_ask(SymbolId symbol) const noexcept;

    [[nodiscard]] const std::vector<SymbolId>& symbols() const noexcept { return seen_; }
    [[nodiscard]] std::size_t symbol_count() const noexcept { return seen_.size(); }
    [[nodiscard]] std::size_t total_orders() const noexcept;
    [[nodiscard]] std::size_t per_symbol_capacity() const noexcept { return per_symbol_capacity_; }

    // Order-independent 64-bit checksum over all per-symbol books, keyed by symbol.
    [[nodiscard]] std::uint64_t state_checksum() const noexcept;

private:
    std::vector<std::unique_ptr<OrderBook>> books_;  // indexed by symbol id
    std::vector<SymbolId> seen_;                     // symbols with a book, first-seen order
    std::size_t per_symbol_capacity_;
    std::size_t max_symbols_;
};

}  // namespace market
