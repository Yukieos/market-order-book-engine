#pragma once

#include "market/DataConnector.hpp"
#include "market/itch/DatagramSource.hpp"
#include "market/itch/MoldUdp64.hpp"

#include <cstdint>
#include <memory>

namespace market::itch {

// Decodes a MoldUDP64 stream (pulled datagram-by-datagram from a DatagramSource)
// into MarketDataEvents for apply mode, reusing itch::decode_message. Tracks the
// per-message sequence numbers the framing carries and reports gaps (missed
// messages) and overlaps (retransmits, whose already-seen messages are skipped).
class MoldUdp64Connector final : public DataConnector {
public:
    explicit MoldUdp64Connector(std::unique_ptr<DatagramSource> source) noexcept;

    bool next(MarketDataEvent& event) override;

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] bool end_of_session() const noexcept { return end_of_session_; }
    [[nodiscard]] std::size_t messages_decoded() const noexcept { return messages_decoded_; }
    [[nodiscard]] std::uint64_t gaps_detected() const noexcept { return gaps_detected_; }
    [[nodiscard]] std::uint64_t messages_missed() const noexcept { return messages_missed_; }
    [[nodiscard]] std::uint64_t last_sequence() const noexcept { return last_sequence_; }

private:
    bool load_next_packet();

    std::unique_ptr<DatagramSource> source_;
    MoldUdp64Packet packet_{};
    std::size_t offset_{0};
    std::uint16_t local_index_{0};
    bool have_packet_{false};
    bool initialized_{false};
    bool failed_{false};
    bool end_of_session_{false};
    std::uint64_t expected_{0};
    std::uint64_t last_sequence_{0};
    std::uint64_t gaps_detected_{0};
    std::uint64_t messages_missed_{0};
    std::size_t messages_decoded_{0};
};

}  // namespace market::itch
