#pragma once

#include "market/MarketDataEvent.hpp"

#include <cstdint>

namespace market {

// Matching-mode order intent (see ARCHITECTURE.md section 3). These describe an
// incoming client order that the book will *match*, as opposed to MarketDataEvent
// which replays an exchange-decided feed.
enum class OrderType : std::uint8_t { Limit, Market };
enum class TimeInForce : std::uint8_t { GTC, IOC, FOK };
enum class RequestType : std::uint8_t { New, Cancel };

struct OrderRequest {
    RequestType type{RequestType::New};
    OrderId id{};             // client order id; the taker id for a New order
    Side side{};
    Price price_ticks{};      // ignored for Market orders
    Quantity quantity{};
    OrderType order_type{OrderType::Limit};
    TimeInForce tif{TimeInForce::GTC};

    friend bool operator==(const OrderRequest&, const OrderRequest&) = default;
};

// One maker/taker match. price_ticks is the resting maker's price (price-time
// priority: the taker never trades at a worse price than its own limit).
struct Fill {
    OrderId taker_id{};
    OrderId maker_id{};
    Side taker_side{};
    Price price_ticks{};
    Quantity quantity{};
    std::uint64_t sequence{};

    friend bool operator==(const Fill&, const Fill&) = default;
};

// Allocation-free, cross-translation-unit, noexcept output sink for fills
// (function pointer + opaque context). Keeps submit() off the heap and inlinable
// at the call site while still allowing OrderBook to live in its own .cpp.
struct FillSink {
    void* ctx{nullptr};
    void (*fn)(void*, const Fill&) noexcept {nullptr};
    void operator()(const Fill& fill) const noexcept {
        if (fn != nullptr) fn(ctx, fill);
    }
};

// Outcome of submit(). The remainder disposition and fill amount are both encoded
// so tests and callers can assert exact behavior without inspecting the book.
enum class SubmitResult : std::uint8_t {
    FilledComplete,      // remaining == 0
    RestedNoFill,        // no cross; whole order rests (Limit GTC)
    PartialFillRested,   // some filled; remainder rests (Limit GTC)
    PartialFillKilled,   // some filled; remainder killed (Market / IOC)
    NoFillKilled,        // no cross; nothing rests (Market / IOC)
    Canceled,            // RequestType::Cancel applied
    RejectedFOK,         // FOK could not fully fill; nothing done
    RejectedDuplicate,   // id collides with a resting order
    RejectedCapacity,    // no pool slot to rest the remainder
    RejectedInvalid      // zero quantity, or Cancel of an unknown id
};

}  // namespace market
