#include "market/itch/MoldUdp64Connector.hpp"

#include "market/itch/DatagramSource.hpp"

#include <ios>
#include <utility>

namespace market::itch {

// --- LengthPrefixedDatagramFileSource (streaming, one datagram at a time) ---

LengthPrefixedDatagramFileSource::LengthPrefixedDatagramFileSource(
    const std::filesystem::path& path)
    : input_(path, std::ios::binary) {
    if (!input_) failed_ = true;
}

std::optional<std::span<const std::byte>>
LengthPrefixedDatagramFileSource::next_datagram() {
    if (failed_ || !input_) return std::nullopt;
    std::byte length_bytes[4];
    input_.read(reinterpret_cast<char*>(length_bytes), 4);
    const auto header_read = input_.gcount();
    if (header_read == 0 && input_.eof()) return std::nullopt;  // clean end of file
    if (header_read != 4) {
        failed_ = true;
        return std::nullopt;
    }
    const std::size_t length = read_be32(length_bytes);
    buffer_.resize(length);
    if (length > 0) {
        input_.read(reinterpret_cast<char*>(buffer_.data()), static_cast<std::streamsize>(length));
        if (static_cast<std::size_t>(input_.gcount()) != length) {
            failed_ = true;
            return std::nullopt;
        }
    }
    return std::span<const std::byte>(buffer_);
}

// --- MoldUdp64Connector ---

MoldUdp64Connector::MoldUdp64Connector(std::unique_ptr<DatagramSource> source) noexcept
    : source_(std::move(source)) {}

bool MoldUdp64Connector::load_next_packet() {
    for (;;) {
        const auto datagram = source_->next_datagram();
        if (!datagram) {
            if (source_->failed()) failed_ = true;
            return false;
        }
        const auto parsed = MoldUdp64Packet::parse(*datagram);
        if (!parsed) {
            failed_ = true;
            return false;
        }
        packet_ = *parsed;
        offset_ = kMoldHeaderSize;
        local_index_ = 0;
        if (packet_.end_of_session()) {
            end_of_session_ = true;
            continue;
        }
        if (packet_.heartbeat()) continue;

        const std::uint64_t seq = packet_.sequence();
        if (!initialized_) {
            initialized_ = true;
            expected_ = seq;
        }
        if (seq > expected_) {  // gap: sequence numbers were skipped
            messages_missed_ += seq - expected_;
            ++gaps_detected_;
            expected_ = seq;
        } else if (seq < expected_) {  // overlap: skip messages already delivered
            std::uint64_t already_seen = expected_ - seq;
            while (already_seen > 0 && local_index_ < packet_.message_count()) {
                std::span<const std::byte> skipped;
                if (!packet_.read_block(offset_, skipped)) {
                    failed_ = true;
                    return false;
                }
                ++local_index_;
                --already_seen;
            }
            if (local_index_ >= packet_.message_count()) continue;  // wholly a retransmit
        }
        have_packet_ = true;
        return true;
    }
}

bool MoldUdp64Connector::next(MarketDataEvent& event) {
    for (;;) {
        if (!have_packet_ || local_index_ >= packet_.message_count()) {
            have_packet_ = false;
            if (!load_next_packet()) return false;
        }
        std::span<const std::byte> payload;
        if (!packet_.read_block(offset_, payload)) {
            failed_ = true;
            return false;
        }
        const std::uint64_t message_sequence = packet_.sequence() + local_index_;
        ++local_index_;
        expected_ = message_sequence + 1;
        last_sequence_ = message_sequence;

        const DecodeResult result = decode_message(payload.data(), payload.size());
        if (result.status == DecodeStatus::Malformed) {
            failed_ = true;
            return false;
        }
        if (result.status == DecodeStatus::Event) {
            event = result.event;
            event.sequence = message_sequence;
            ++messages_decoded_;
            return true;
        }
        // Skipped (non-book message): continue to the next block.
    }
}

}  // namespace market::itch
