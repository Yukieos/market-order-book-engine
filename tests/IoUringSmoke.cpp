// Loopback smoke test for the io_uring UDP datagram source. Built and run only when
// MARKET_ENABLE_IO_URING is on (Linux CI). Sends a few MoldUDP64 datagrams to the
// bound port, then reads them back through MoldUdp64Connector over io_uring.
#include "market/itch/IoUringDatagramSource.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

using namespace market;

namespace {

#define CHECK(cond) do { if (!(cond)) throw std::runtime_error("CHECK failed: " #cond); } while (false)

void put_be(std::vector<std::byte>& out, std::uint64_t value, int bytes) {
    for (int i = bytes - 1; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((value >> (8 * i)) & 0xFFULL));
    }
}

std::vector<std::byte> add_message(std::uint64_t id, char side, std::uint32_t shares,
                                   std::uint32_t price) {
    std::vector<std::byte> m;
    m.push_back(std::byte{'A'});
    put_be(m, 0, 2);
    put_be(m, 0, 2);
    put_be(m, 0, 6);
    put_be(m, id, 8);
    m.push_back(static_cast<std::byte>(side));
    put_be(m, shares, 4);
    for (int i = 0; i < 8; ++i) m.push_back(std::byte{' '});
    put_be(m, price, 4);
    return m;
}

std::vector<std::byte> mold_packet(std::uint64_t sequence,
                                   const std::vector<std::vector<std::byte>>& messages) {
    std::vector<std::byte> p;
    for (int i = 0; i < 10; ++i) p.push_back(std::byte{0});  // session
    put_be(p, sequence, 8);
    put_be(p, static_cast<std::uint64_t>(messages.size()), 2);
    for (const auto& m : messages) {
        put_be(p, static_cast<std::uint64_t>(m.size()), 2);
        p.insert(p.end(), m.begin(), m.end());
    }
    return p;
}

void send_to(std::uint16_t port, const std::vector<std::byte>& bytes) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("sender socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    ::sendto(fd, bytes.data(), bytes.size(), 0, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::close(fd);
}

}  // namespace

int main() {
    try {
        auto source = std::make_unique<itch::IoUringDatagramSource>("127.0.0.1", 0);
        const std::uint16_t port = source->bound_port();
        CHECK(port != 0);

        // Two packets (sequences 1..2 and 3), then a zero-length sentinel = end.
        send_to(port, mold_packet(1, {add_message(10, 'B', 100, 1000),
                                      add_message(11, 'S', 50, 1020)}));
        send_to(port, mold_packet(3, {add_message(12, 'B', 25, 999)}));
        send_to(port, {});  // sentinel

        itch::MoldUdp64Connector connector(std::move(source));
        MarketDataEvent event{};
        std::vector<OrderId> ids;
        while (connector.next(event)) ids.push_back(event.order_id);

        CHECK(!connector.failed());
        CHECK(connector.messages_decoded() == 3);
        CHECK(connector.gaps_detected() == 0);
        CHECK(ids.size() == 3);
        CHECK(ids[0] == 10 && ids[1] == 11 && ids[2] == 12);

        std::cout << "io_uring smoke test passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
