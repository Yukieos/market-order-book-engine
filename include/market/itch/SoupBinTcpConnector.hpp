#pragma once

#include "market/DataConnector.hpp"
#include "market/itch/ItchDecoder.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <vector>

namespace market::itch {

// Frames a SoupBinTCP stream and decodes the ITCH payload of each Sequenced Data
// packet ('S'). SoupBinTCP is NASDAQ's TCP session layer: each packet is a 2-byte
// big-endian length (counting the 1-byte packet type + payload) followed by the type
// and payload. Sequenced Data packets carry one ITCH message and advance an implicit
// sequence number; heartbeats/login/debug/unsequenced packets are skipped; an End of
// Session packet ('Z') ends the stream cleanly.
//
// This handles the framing/decoding, which is the reusable core; establishing the TCP
// session (login handshake, heartbeats) is out of scope. Same bounded-window streaming
// framer as ItchFileConnector, so day-sized captures need not be resident.
class SoupBinTcpConnector final : public DataConnector {
public:
    explicit SoupBinTcpConnector(std::vector<std::byte> data) noexcept;
    explicit SoupBinTcpConnector(const std::filesystem::path& path);

    bool next(MarketDataEvent& event) override;

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] bool end_of_session() const noexcept { return end_of_session_; }
    [[nodiscard]] std::size_t messages_decoded() const noexcept { return messages_decoded_; }

private:
    bool ensure(std::size_t n);

    std::vector<std::byte> buffer_;
    std::ifstream input_;
    std::size_t pos_{0};
    std::size_t size_{0};
    std::uint64_t sequence_{0};
    std::size_t messages_decoded_{0};
    bool eof_{false};
    bool failed_{false};
    bool end_of_session_{false};
};

}  // namespace market::itch
