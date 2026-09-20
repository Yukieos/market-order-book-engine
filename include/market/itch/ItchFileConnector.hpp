#pragma once

#include "market/DataConnector.hpp"
#include "market/itch/ItchDecoder.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <vector>

namespace market::itch {

// Replays a NASDAQ BinaryFILE-style stream in which each ITCH 5.0 message is
// preceded by a 2-byte big-endian length. Book-affecting messages are normalized
// to MarketDataEvent and delivered one per next() call (apply mode); non-book
// messages are skipped. A malformed or truncated frame stops the stream (failed()).
//
// Two sources share one streaming framer:
//   - from a byte vector (held whole; used by tests and benchmarks), and
//   - from a file, read in bounded chunks so day-sized captures need not fit in RAM.
// next() performs no heap allocation in steady state (the read buffer is fixed and
// only grows for a message larger than the buffer, which ITCH never produces).
class ItchFileConnector final : public DataConnector {
public:
    explicit ItchFileConnector(std::vector<std::byte> data) noexcept;
    explicit ItchFileConnector(const std::filesystem::path& path);

    bool next(MarketDataEvent& event) override;

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] std::size_t messages_decoded() const noexcept { return messages_decoded_; }
    [[nodiscard]] std::size_t bytes_consumed() const noexcept { return bytes_consumed_; }

private:
    // Guarantees at least `n` unread bytes at pos_, compacting and refilling from the
    // stream as needed. Returns false when the stream cannot provide that many.
    bool ensure(std::size_t n);

    std::vector<std::byte> buffer_;
    std::ifstream input_;
    std::size_t pos_{0};
    std::size_t size_{0};  // number of valid bytes currently in buffer_
    std::uint64_t sequence_{0};
    std::size_t messages_decoded_{0};
    std::size_t bytes_consumed_{0};
    bool eof_{false};  // the stream will yield no further bytes
    bool failed_{false};
};

}  // namespace market::itch
