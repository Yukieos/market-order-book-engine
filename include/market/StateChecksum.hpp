#pragma once

#include "market/OrderBook.hpp"

#include <bit>
#include <cstdint>
#include <span>

namespace market {

constexpr std::uint64_t checksum_mix(std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return value;
}

constexpr std::uint64_t order_checksum(const OrderView& order) noexcept {
    auto value = checksum_mix(order.id);
    value ^= std::rotl(checksum_mix(static_cast<std::uint64_t>(order.price_ticks)), 11);
    value ^= std::rotl(checksum_mix(order.quantity), 23);
    value ^= std::rotl(checksum_mix(order.priority), 37);
    value ^= order.side == Side::Buy ? 0x6a09e667f3bcc909ULL : 0xbb67ae8584caa73bULL;
    return checksum_mix(value);
}

inline std::uint64_t state_checksum(std::span<const OrderView> orders) noexcept {
    std::uint64_t result = checksum_mix(static_cast<std::uint64_t>(orders.size()));
    for (const auto& order : orders) result ^= order_checksum(order);
    return result;
}

}  // namespace market

