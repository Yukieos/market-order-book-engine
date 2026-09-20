#include "market/itch/ItchFileConnector.hpp"

#include <cstring>
#include <ios>

namespace market::itch {
namespace {
constexpr std::size_t kStreamBufferSize = 1u << 16;  // 64 KiB read window
}

// In-memory: the buffer holds the whole stream and no refill is possible.
ItchFileConnector::ItchFileConnector(std::vector<std::byte> data) noexcept
    : buffer_(std::move(data)), size_(buffer_.size()), eof_(true) {}

// File: a fixed read window is refilled from the stream on demand.
ItchFileConnector::ItchFileConnector(const std::filesystem::path& path)
    : buffer_(kStreamBufferSize), input_(path, std::ios::binary) {
    if (!input_) {
        failed_ = true;
        eof_ = true;
    }
}

bool ItchFileConnector::ensure(std::size_t n) {
    if (size_ - pos_ >= n) return true;
    if (eof_) return false;

    const std::size_t remaining = size_ - pos_;
    if (pos_ > 0) {
        if (remaining > 0) std::memmove(buffer_.data(), buffer_.data() + pos_, remaining);
        pos_ = 0;
        size_ = remaining;
    }
    if (buffer_.size() < n) buffer_.resize(n);  // only if a single message exceeds the window
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

bool ItchFileConnector::next(MarketDataEvent& event) {
    for (;;) {
        if (!ensure(2)) {
            if (size_ - pos_ != 0) failed_ = true;  // trailing partial length prefix
            return false;
        }
        const std::size_t length = read_be16(buffer_.data() + pos_);
        if (length == 0) {
            failed_ = true;
            return false;
        }
        if (!ensure(2 + length)) {  // truncated message body
            failed_ = true;
            return false;
        }
        const DecodeResult result = decode_message(buffer_.data() + pos_ + 2, length);
        pos_ += 2 + length;
        bytes_consumed_ += 2 + length;
        if (result.status == DecodeStatus::Malformed) {
            failed_ = true;
            return false;
        }
        if (result.status == DecodeStatus::Event) {
            event = result.event;
            event.sequence = ++sequence_;
            ++messages_decoded_;
            return true;
        }
        // Skipped (non-book message): advance to the next framed message.
    }
}

}  // namespace market::itch
