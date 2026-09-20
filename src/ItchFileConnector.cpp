#include "market/itch/ItchFileConnector.hpp"

#include <fstream>
#include <iterator>

namespace market::itch {

ItchFileConnector::ItchFileConnector(std::vector<std::byte> data) noexcept
    : data_(std::move(data)) {}

ItchFileConnector::ItchFileConnector(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        failed_ = true;
        return;
    }
    input.unsetf(std::ios::skipws);
    const std::istreambuf_iterator<char> begin(input);
    const std::istreambuf_iterator<char> end;
    for (auto it = begin; it != end; ++it) {
        data_.push_back(static_cast<std::byte>(static_cast<unsigned char>(*it)));
    }
}

bool ItchFileConnector::next(MarketDataEvent& event) {
    while (offset_ < data_.size()) {
        // Each message is framed by a 2-byte big-endian length.
        if (data_.size() - offset_ < 2) {
            failed_ = true;  // trailing partial frame
            return false;
        }
        const std::size_t length = read_be16(data_.data() + offset_);
        const std::size_t body = offset_ + 2;
        if (length == 0 || body + length > data_.size()) {
            failed_ = true;  // zero-length or truncated message
            return false;
        }

        const DecodeResult result = decode_message(data_.data() + body, length);
        offset_ = body + length;
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
        // Skipped: advance to the next framed message.
    }
    return false;
}

}  // namespace market::itch
