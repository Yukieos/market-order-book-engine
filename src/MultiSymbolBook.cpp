#include "market/MultiSymbolBook.hpp"

#include "market/StateChecksum.hpp"

namespace market {

MultiSymbolBook::MultiSymbolBook(std::size_t per_symbol_capacity, std::size_t max_symbols)
    : books_(max_symbols),
      per_symbol_capacity_(per_symbol_capacity),
      max_symbols_(max_symbols) {}

ProcessResult MultiSymbolBook::process_event(const MarketDataEvent& event) {
    if (event.symbol >= max_symbols_) return ProcessResult::CapacityExhausted;
    auto& slot = books_[event.symbol];
    if (!slot) {
        slot = std::make_unique<OrderBook>(per_symbol_capacity_);
        seen_.push_back(event.symbol);
    }
    return slot->process_event(event);
}

const OrderBook* MultiSymbolBook::book(SymbolId symbol) const noexcept {
    if (symbol >= max_symbols_) return nullptr;
    return books_[symbol].get();
}

std::optional<LevelView> MultiSymbolBook::best_bid(SymbolId symbol) const noexcept {
    const OrderBook* b = book(symbol);
    return b ? b->best_bid() : std::nullopt;
}

std::optional<LevelView> MultiSymbolBook::best_ask(SymbolId symbol) const noexcept {
    const OrderBook* b = book(symbol);
    return b ? b->best_ask() : std::nullopt;
}

std::size_t MultiSymbolBook::total_orders() const noexcept {
    std::size_t total = 0;
    for (const auto symbol : seen_) total += books_[symbol]->order_count();
    return total;
}

std::uint64_t MultiSymbolBook::state_checksum() const noexcept {
    std::uint64_t result = 0;
    // XOR keeps this order-independent; mixing the symbol id keeps identical books
    // on different symbols from cancelling out.
    for (const auto symbol : seen_) {
        result ^= checksum_mix(symbol) ^ books_[symbol]->state_checksum();
    }
    return result;
}

}  // namespace market
