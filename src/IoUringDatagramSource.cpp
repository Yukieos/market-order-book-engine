#include "market/itch/IoUringDatagramSource.hpp"

#include <liburing.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace market::itch {

struct IoUringDatagramSource::Impl {
    io_uring ring{};
    int fd{-1};
    std::vector<std::byte> buffer;
    bool ring_ready{false};

    ~Impl() {
        if (ring_ready) io_uring_queue_exit(&ring);
        if (fd >= 0) ::close(fd);
    }
};

IoUringDatagramSource::IoUringDatagramSource(const std::string& bind_addr, std::uint16_t port,
                                             unsigned queue_depth)
    : impl_(std::make_unique<Impl>()) {
    impl_->buffer.resize(2048);  // one datagram fits comfortably (MoldUDP64 <= MTU)

    impl_->fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (impl_->fd < 0) throw std::runtime_error("io_uring: socket() failed");

    int reuse = 1;
    ::setsockopt(impl_->fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    int rcvbuf = 4 << 20;  // request 4 MiB so short bursts are not dropped (kernel may cap)
    ::setsockopt(impl_->fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1) {
        throw std::runtime_error("io_uring: invalid bind address");
    }
    if (::bind(impl_->fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        throw std::runtime_error("io_uring: bind() failed");
    }

    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    if (::getsockname(impl_->fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
        bound_port_ = ntohs(bound.sin_port);
    } else {
        bound_port_ = port;
    }

    if (io_uring_queue_init(queue_depth, &impl_->ring, 0) != 0) {
        throw std::runtime_error("io_uring: queue_init failed");
    }
    impl_->ring_ready = true;
}

IoUringDatagramSource::~IoUringDatagramSource() = default;

std::optional<std::span<const std::byte>> IoUringDatagramSource::next_datagram() {
    if (failed_) return std::nullopt;

    io_uring_sqe* sqe = io_uring_get_sqe(&impl_->ring);
    if (sqe == nullptr) {
        failed_ = true;
        return std::nullopt;
    }
    io_uring_prep_recv(sqe, impl_->fd, impl_->buffer.data(), impl_->buffer.size(), 0);
    if (io_uring_submit(&impl_->ring) < 0) {
        failed_ = true;
        return std::nullopt;
    }

    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&impl_->ring, &cqe) < 0) {
        failed_ = true;
        return std::nullopt;
    }
    const int result = cqe->res;
    io_uring_cqe_seen(&impl_->ring, cqe);

    if (result <= 0) return std::nullopt;  // 0-length sentinel or receive error: end of stream
    return std::span<const std::byte>(impl_->buffer.data(), static_cast<std::size_t>(result));
}

}  // namespace market::itch
