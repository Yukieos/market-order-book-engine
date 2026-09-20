#pragma once

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <vector>

namespace market::itch {

// A source of discrete datagrams (one MoldUDP64 packet each). Decouples the
// transport (in-memory, file replay, or an io_uring UDP socket) from MoldUDP64
// framing and ITCH decoding. See ARCHITECTURE.md section 7.
class DatagramSource {
public:
    virtual ~DatagramSource() = default;

    // Returns the next datagram as a view valid until the next call, or nullopt at
    // end of stream. Implementations must keep the returned bytes stable until the
    // caller asks for the next datagram.
    virtual std::optional<std::span<const std::byte>> next_datagram() = 0;

    [[nodiscard]] virtual bool failed() const noexcept { return false; }
};

// Owns a fixed list of datagrams; returned spans stay valid for the source's
// lifetime. Useful for tests and for feeding pre-captured packets.
class InMemoryDatagramSource final : public DatagramSource {
public:
    explicit InMemoryDatagramSource(std::vector<std::vector<std::byte>> datagrams) noexcept
        : datagrams_(std::move(datagrams)) {}

    std::optional<std::span<const std::byte>> next_datagram() override {
        if (index_ >= datagrams_.size()) return std::nullopt;
        return std::span<const std::byte>(datagrams_[index_++]);
    }

private:
    std::vector<std::vector<std::byte>> datagrams_;
    std::size_t index_{0};
};

// Streams datagrams from a file where each datagram is stored as a 4-byte
// big-endian length followed by that many bytes. One datagram is read into an
// internal buffer at a time (valid until the next call), so day-sized captures
// do not need to be held in memory.
class LengthPrefixedDatagramFileSource final : public DatagramSource {
public:
    explicit LengthPrefixedDatagramFileSource(const std::filesystem::path& path);

    std::optional<std::span<const std::byte>> next_datagram() override;
    [[nodiscard]] bool failed() const noexcept override { return failed_; }

private:
    std::ifstream input_;
    std::vector<std::byte> buffer_;
    bool failed_{false};
};

}  // namespace market::itch
