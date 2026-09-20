#pragma once

#include "market/itch/ItchDecoder.hpp"  // read_be16 / read_be64

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

// MoldUDP64 is NASDAQ's UDP framing for a stream of ITCH messages. Each downstream
// packet (one datagram) carries a 20-byte header followed by length-framed message
// blocks. The header's sequence number is the sequence of the first message in the
// packet, which lets a receiver detect gaps and retransmits. See ARCHITECTURE.md
// section 4.
namespace market::itch {

inline constexpr std::size_t kMoldHeaderSize = 20;   // session(10) + sequence(8) + count(2)
inline constexpr std::uint16_t kMoldEndOfSession = 0xFFFF;

// Zero-copy reader over one MoldUDP64 downstream packet.
class MoldUdp64Packet {
public:
    [[nodiscard]] static std::optional<MoldUdp64Packet> parse(
        std::span<const std::byte> datagram) noexcept {
        if (datagram.size() < kMoldHeaderSize) return std::nullopt;
        MoldUdp64Packet packet;
        packet.data_ = datagram;
        packet.sequence_ = read_be64(datagram.data() + 10);
        packet.count_ = read_be16(datagram.data() + 18);
        return packet;
    }

    [[nodiscard]] std::uint64_t sequence() const noexcept { return sequence_; }
    [[nodiscard]] std::uint16_t message_count() const noexcept { return count_; }
    [[nodiscard]] bool end_of_session() const noexcept { return count_ == kMoldEndOfSession; }
    [[nodiscard]] bool heartbeat() const noexcept { return count_ == 0; }

    // Read the message block at byte `offset`; on success set `out` to the payload
    // and advance `offset` past the block. Returns false on truncated framing.
    [[nodiscard]] bool read_block(std::size_t& offset,
                                  std::span<const std::byte>& out) const noexcept {
        if (offset + 2 > data_.size()) return false;
        const std::size_t length = read_be16(data_.data() + offset);
        if (offset + 2 + length > data_.size()) return false;
        out = data_.subspan(offset + 2, length);
        offset += 2 + length;
        return true;
    }

private:
    std::span<const std::byte> data_{};
    std::uint64_t sequence_{0};
    std::uint16_t count_{0};
};

}  // namespace market::itch
