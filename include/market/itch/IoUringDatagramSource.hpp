#pragma once

#include "market/itch/DatagramSource.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>

// A DatagramSource that receives UDP datagrams (each a MoldUDP64 packet) via Linux
// io_uring, so the receive path avoids the read() syscall per packet. Built only
// when MARKET_ENABLE_IO_URING is on (needs liburing); the liburing header is kept
// out of this interface via a pimpl. A zero-length datagram is treated as
// end-of-stream, which the replay/smoke tooling uses as a sentinel.
namespace market::itch {

class IoUringDatagramSource final : public DatagramSource {
public:
    // Bind a UDP socket to bind_addr:port ("127.0.0.1" for loopback). A port of 0
    // asks the OS to choose one; read it back with bound_port(). Throws
    // std::runtime_error on setup failure.
    IoUringDatagramSource(const std::string& bind_addr, std::uint16_t port,
                          unsigned queue_depth = 256);
    ~IoUringDatagramSource() override;

    IoUringDatagramSource(const IoUringDatagramSource&) = delete;
    IoUringDatagramSource& operator=(const IoUringDatagramSource&) = delete;

    std::optional<std::span<const std::byte>> next_datagram() override;
    [[nodiscard]] bool failed() const noexcept override { return failed_; }
    [[nodiscard]] std::uint16_t bound_port() const noexcept { return bound_port_; }

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::uint16_t bound_port_{0};
    bool failed_{false};
};

}  // namespace market::itch
