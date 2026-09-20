#pragma once

#include <cstdint>
#include <string_view>

namespace market {

using OrderId = std::uint64_t;
using Price = std::int64_t;
using Quantity = std::uint64_t;

// Add/Modify/Cancel/Trade come from Milestone 1. Reduce and Replace were added for
// real matched feeds (e.g. NASDAQ ITCH 5.0; see ARCHITECTURE.md section 2.2):
//   Reduce  - decrement a resting order by `quantity`, remove on reaching 0. Same
//             book mechanics as Trade, but semantically a cancel, not a print.
//   Replace - atomic cancel of `order_id` then add `new_order_id` at the new
//             price/quantity, inheriting the original order's side.
enum class EventType : std::uint8_t { Add, Modify, Cancel, Trade, Reduce, Replace };
enum class Side : std::uint8_t { Buy, Sell };

struct MarketDataEvent {
    std::uint64_t sequence{};
    std::uint64_t exchange_timestamp_ns{};
    EventType type{};
    OrderId order_id{};        // for Replace: the original order reference
    OrderId new_order_id{};    // only meaningful for Replace
    Side side{};
    Price price_ticks{};
    Quantity quantity{};

    friend bool operator==(const MarketDataEvent&, const MarketDataEvent&) = default;
};

constexpr std::string_view to_string(EventType type) noexcept {
    switch (type) {
        case EventType::Add: return "add";
        case EventType::Modify: return "modify";
        case EventType::Cancel: return "cancel";
        case EventType::Trade: return "trade";
        case EventType::Reduce: return "reduce";
        case EventType::Replace: return "replace";
    }
    return "unknown";
}

constexpr std::string_view to_string(Side side) noexcept {
    return side == Side::Buy ? "buy" : "sell";
}

}  // namespace market

