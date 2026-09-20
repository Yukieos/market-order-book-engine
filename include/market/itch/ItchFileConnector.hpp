#pragma once

#include "market/DataConnector.hpp"
#include "market/itch/ItchDecoder.hpp"

#include <cstddef>
#include <filesystem>
#include <vector>

namespace market::itch {

// Replays a NASDAQ BinaryFILE / MoldUDP64-style stream in which each ITCH 5.0
// message is preceded by a 2-byte big-endian length. Book-affecting messages are
// normalized to MarketDataEvent and delivered one per next() call (apply mode);
// non-book messages are skipped. See ARCHITECTURE.md section 4.
//
// The byte buffer is loaded once at construction (setup may allocate, like the CSV
// connector); next() performs no allocation. A malformed frame stops the stream and
// is reported via failed().
class ItchFileConnector final : public DataConnector {
public:
    explicit ItchFileConnector(std::vector<std::byte> data) noexcept;
    explicit ItchFileConnector(const std::filesystem::path& path);

    bool next(MarketDataEvent& event) override;

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] std::size_t messages_decoded() const noexcept { return messages_decoded_; }
    [[nodiscard]] std::size_t bytes_consumed() const noexcept { return offset_; }

private:
    std::vector<std::byte> data_;
    std::size_t offset_{0};
    std::size_t messages_decoded_{0};
    std::uint64_t sequence_{0};
    bool failed_{false};
};

}  // namespace market::itch
