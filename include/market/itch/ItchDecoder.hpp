#pragma once

#include "market/MarketDataEvent.hpp"

#include <cstddef>
#include <cstdint>

// Stateless, allocation-free decoder for the book-affecting subset of NASDAQ
// TotalView-ITCH 5.0. Fields are read at fixed big-endian offsets from the start
// of a single (de-framed) message; see ARCHITECTURE.md section 4. All multi-byte
// integers in ITCH are big-endian; prices are unsigned integers in units of
// 1/10000, mapped directly onto Price ticks.
namespace market::itch {

// Message type tags (first byte of each message).
enum class MessageType : char {
    AddOrder = 'A',
    AddOrderMpid = 'F',
    OrderExecuted = 'E',
    OrderExecutedWithPrice = 'C',
    OrderCancel = 'X',
    OrderDelete = 'D',
    OrderReplace = 'U',
};

enum class DecodeStatus : std::uint8_t {
    Event,      // a book-affecting event was produced
    Skipped,    // a valid message we do not apply to the book (system/trade/NOII/...)
    Malformed,  // length too short for the claimed type, or a bad field
};

struct DecodeResult {
    DecodeStatus status{DecodeStatus::Skipped};
    MarketDataEvent event{};
};

// Big-endian field readers over raw bytes (no alignment or aliasing assumptions).
[[nodiscard]] inline std::uint16_t read_be16(const std::byte* p) noexcept {
    return static_cast<std::uint16_t>((std::to_integer<std::uint16_t>(p[0]) << 8) |
                                       std::to_integer<std::uint16_t>(p[1]));
}

[[nodiscard]] inline std::uint32_t read_be32(const std::byte* p) noexcept {
    return (std::to_integer<std::uint32_t>(p[0]) << 24) |
           (std::to_integer<std::uint32_t>(p[1]) << 16) |
           (std::to_integer<std::uint32_t>(p[2]) << 8) |
           std::to_integer<std::uint32_t>(p[3]);
}

[[nodiscard]] inline std::uint64_t read_be48(const std::byte* p) noexcept {
    std::uint64_t value = 0;
    for (int i = 0; i < 6; ++i) {
        value = (value << 8) | std::to_integer<std::uint64_t>(p[i]);
    }
    return value;
}

[[nodiscard]] inline std::uint64_t read_be64(const std::byte* p) noexcept {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | std::to_integer<std::uint64_t>(p[i]);
    }
    return value;
}

// Minimum message lengths (including the leading type byte) for the subset we apply.
inline constexpr std::size_t kLenAddOrder = 36;
inline constexpr std::size_t kLenAddOrderMpid = 40;
inline constexpr std::size_t kLenOrderExecuted = 31;
inline constexpr std::size_t kLenOrderExecutedWithPrice = 36;
inline constexpr std::size_t kLenOrderCancel = 23;
inline constexpr std::size_t kLenOrderDelete = 19;
inline constexpr std::size_t kLenOrderReplace = 35;

// Decode one de-framed ITCH message. `data` points at the type byte; `len` is the
// message length as given by the framing layer.
[[nodiscard]] inline DecodeResult decode_message(const std::byte* data, std::size_t len) noexcept {
    if (len == 0) return {DecodeStatus::Malformed, {}};
    const auto type = static_cast<char>(std::to_integer<unsigned char>(data[0]));
    MarketDataEvent event{};

    const auto set_side = [](std::byte b, Side& side) noexcept -> bool {
        const char c = static_cast<char>(std::to_integer<unsigned char>(b));
        if (c == 'B') { side = Side::Buy; return true; }
        if (c == 'S') { side = Side::Sell; return true; }
        return false;
    };

    switch (static_cast<MessageType>(type)) {
        case MessageType::AddOrder:
        case MessageType::AddOrderMpid: {
            if (len < kLenAddOrder) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Add;
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);
            if (!set_side(data[19], event.side)) return {DecodeStatus::Malformed, {}};
            event.quantity = read_be32(data + 20);
            event.price_ticks = static_cast<Price>(read_be32(data + 32));
            return {DecodeStatus::Event, event};
        }
        case MessageType::OrderExecuted: {
            if (len < kLenOrderExecuted) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Trade;
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);
            event.quantity = read_be32(data + 19);
            return {DecodeStatus::Event, event};
        }
        case MessageType::OrderExecutedWithPrice: {
            if (len < kLenOrderExecutedWithPrice) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Trade;
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);
            event.quantity = read_be32(data + 19);
            event.price_ticks = static_cast<Price>(read_be32(data + 32));  // execution price
            return {DecodeStatus::Event, event};
        }
        case MessageType::OrderCancel: {
            if (len < kLenOrderCancel) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Reduce;  // partial cancel, not a trade print
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);
            event.quantity = read_be32(data + 19);
            return {DecodeStatus::Event, event};
        }
        case MessageType::OrderDelete: {
            if (len < kLenOrderDelete) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Cancel;
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);
            return {DecodeStatus::Event, event};
        }
        case MessageType::OrderReplace: {
            if (len < kLenOrderReplace) return {DecodeStatus::Malformed, {}};
            event.type = EventType::Replace;
            event.exchange_timestamp_ns = read_be48(data + 5);
            event.order_id = read_be64(data + 11);       // original order reference
            event.new_order_id = read_be64(data + 19);   // new order reference
            event.quantity = read_be32(data + 27);
            event.price_ticks = static_cast<Price>(read_be32(data + 31));
            return {DecodeStatus::Event, event};
        }
    }
    // System event, stock directory, trades, NOII, etc.: valid but not applied here.
    return {DecodeStatus::Skipped, {}};
}

}  // namespace market::itch
