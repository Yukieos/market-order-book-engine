#include "market/itch/SoupBinTcpConnector.hpp"

#include <cstring>
#include <ios>

namespace market::itch {
namespace {
constexpr std::size_t kStreamBufferSize = 1u << 16;
}

SoupBinTcpConnector::SoupBinTcpConnector(std::vector<std::byte> data) noexcept
    : buffer_(std::move(data)), size_(buffer_.size()), eof_(true) {}

SoupBinTcpConnector::SoupBinTcpConnector(const std::filesystem::path& path)
    : buffer_(kStreamBufferSize), input_(path, std::ios::binary) {
    if (!input_) {
        failed_ = true;
        eof_ = true;
    }
}

// Same bounded-window refill as ItchFileConnector::ensure.
bool SoupBinTcpConnector::ensure(std::size_t n) {
    if (size_ - pos_ >= n) return true;
    if (eof_) return false;
    const std::size_t remaining = size_ - pos_;
    if (pos_ > 0) {
        if (remaining > 0) std::memmove(buffer_.data(), buffer_.data() + pos_, remaining);
        pos_ = 0;
        size_ = remaining;
    }
    if (buffer_.size() < n) buffer_.resize(n);
    while (size_ < n && !eof_) {
        if (size_ == buffer_.size()) buffer_.resize(buffer_.size() * 2);
        input_.read(reinterpret_cast<char*>(buffer_.data() + size_),
                    static_cast<std::streamsize>(buffer_.size() - size_));
        const auto got = input_.gcount();
        if (got <= 0) {
            eof_ = true;
            break;
        }
        size_ += static_cast<std::size_t>(got);
    }
    return (size_ - pos_) >= n;
}

bool SoupBinTcpConnector::next(MarketDataEvent& event) {
    for (;;) {
        if (!ensure(2)) {
            if (size_ - pos_ != 0) failed_ = true;  // trailing partial length prefix
            return false;
        }
        const std::size_t length = read_be16(buffer_.data() + pos_);  // type byte + payload
        if (length == 0) {  // a packet must carry at least a type byte
            failed_ = true;
            return false;
        }
        if (!ensure(2 + length)) {
            failed_ = true;
            return false;
        }
        const char type = static_cast<char>(std::to_integer<unsigned char>(buffer_[pos_ + 2]));
        const std::byte* payload = buffer_.data() + pos_ + 3;
        const std::size_t payload_length = length - 1;
        pos_ += 2 + length;

        if (type == 'Z') {  // End of Session
            end_of_session_ = true;
            return false;
        }
        if (type == 'S') {  // Sequenced Data: one ITCH message, advances the sequence
            const std::uint64_t sequence = ++sequence_;
            const DecodeResult result = decode_message(payload, payload_length);
            if (result.status == DecodeStatus::Malformed) {
                failed_ = true;
                return false;
            }
            if (result.status == DecodeStatus::Event) {
                event = result.event;
                event.sequence = sequence;
                ++messages_decoded_;
                return true;
            }
            continue;  // sequenced but non-book message (e.g. system event)
        }
        // Heartbeat ('H'/'R'), login/logout ('L'/'O'/'A'/'J'), debug ('+'),
        // unsequenced ('U'): not part of the book stream, skip.
    }
}

}  // namespace market::itch
