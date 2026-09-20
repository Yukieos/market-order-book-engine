#pragma once

#include "market/DataConnector.hpp"
#include "market/itch/DatagramSource.hpp"
#include "market/itch/MoldUdp64.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace market::itch {

// Supplies the raw ITCH message payload for a missing sequence number, modelling a
// MoldUDP64 rewind/retransmission server. On a detected gap the connector requests
// each missing sequence in order and splices recovered messages into the delivered
// stream, so downstream sees a gap-free feed. A real deployment would back this with
// a UDP request/response channel to the exchange's retransmit host.
class RetransmitSource {
public:
    virtual ~RetransmitSource() = default;
    // Returns the payload for `sequence` (valid until the next call), or nullopt if it
    // cannot be recovered (then the connector counts it as genuinely missed).
    virtual std::optional<std::span<const std::byte>> recover(std::uint64_t sequence) = 0;
};

// Decodes a MoldUDP64 stream (pulled datagram-by-datagram from a DatagramSource)
// into MarketDataEvents for apply mode, reusing itch::decode_message. Tracks the
// per-message sequence numbers the framing carries and reports gaps (missed
// messages) and overlaps (retransmits, whose already-seen messages are skipped).
class MoldUdp64Connector final : public DataConnector {
public:
    // `retransmit` is optional: when supplied, detected gaps are recovered from it
    // and spliced into the delivered stream; when null, gaps are only counted.
    explicit MoldUdp64Connector(std::unique_ptr<DatagramSource> source,
                                RetransmitSource* retransmit = nullptr) noexcept;

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
    RetransmitSource* retransmit_{nullptr};
    MoldUdp64Packet packet_{};
    std::size_t offset_{0};
    std::uint16_t local_index_{0};
    bool have_packet_{false};
    bool initialized_{false};
    bool failed_{false};
    bool end_of_session_{false};
    bool recovering_{false};
    std::uint64_t recover_next_{0};
    std::uint64_t recover_end_{0};
    std::uint64_t expected_{0};
    std::uint64_t last_sequence_{0};
    std::uint64_t gaps_detected_{0};
    std::uint64_t messages_missed_{0};
    std::size_t messages_decoded_{0};
};

}  // namespace market::itch
