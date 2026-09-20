// End-to-end pipeline benchmark: a UDP sender -> io_uring receive -> MoldUDP64
// decode (producer thread) -> lock-free SPSC queue -> order book (consumer thread).
// Reports the internal transport->book latency decomposition (queue residence, book
// processing, end-to-end) in nanoseconds via the calibrated cycle counter. Built only
// when MARKET_ENABLE_IO_URING is on (Linux). This is a rough, single-machine number
// on whatever host runs it, not a bare-metal guarantee.
#include "market/Affinity.hpp"
#include "market/OrderBook.hpp"
#include "market/SpscQueue.hpp"
#include "market/Time.hpp"
#include "market/itch/IoUringDatagramSource.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

using namespace market;

namespace {

constexpr std::size_t kMessages = 100'000;
constexpr double kTargetRate = 120'000.0;  // offered messages/second (paced; kept modest
                                           // so a shared VM's single-recv path keeps up)

// Cross-core cycle counters are not guaranteed perfectly monotonic on a virtualized
// host, so a delta can come out slightly negative; clamp it to zero.
std::uint64_t delta(std::uint64_t end, std::uint64_t start) {
    return end >= start ? end - start : 0;
}

struct PipeItem {
    MarketDataEvent event{};
    std::uint64_t ingress_cycles{};
    bool stop{false};
};

void put_be(std::vector<std::byte>& out, std::uint64_t value, int bytes) {
    for (int i = bytes - 1; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((value >> (8 * i)) & 0xFFULL));
    }
}

std::vector<std::byte> mold_add_packet(std::uint64_t sequence, std::uint64_t order_id) {
    std::vector<std::byte> add;  // ITCH Add (36 bytes)
    add.push_back(std::byte{'A'});
    put_be(add, 0, 2);
    put_be(add, 0, 2);
    put_be(add, 0, 6);
    put_be(add, order_id, 8);
    add.push_back(std::byte{'B'});
    put_be(add, 1 + order_id % 100, 4);
    for (int i = 0; i < 8; ++i) add.push_back(std::byte{' '});
    put_be(add, 9'900 + order_id % 200, 4);

    std::vector<std::byte> packet;
    for (int i = 0; i < 10; ++i) packet.push_back(std::byte{0});  // session
    put_be(packet, sequence, 8);
    put_be(packet, 1, 2);  // one message
    put_be(packet, static_cast<std::uint64_t>(add.size()), 2);
    packet.insert(packet.end(), add.begin(), add.end());
    return packet;
}

struct Stats {
    std::uint64_t p50{}, p95{}, p99{}, max{};
};

Stats summarize(std::vector<std::uint64_t>& xs, double ns_per_cycle) {
    if (xs.empty()) return {};
    std::sort(xs.begin(), xs.end());
    const auto at = [&](double f) {
        const auto raw = xs[static_cast<std::size_t>(f * static_cast<double>(xs.size() - 1))];
        return static_cast<std::uint64_t>(static_cast<double>(raw) * ns_per_cycle);
    };
    return {at(0.50), at(0.95), at(0.99),
            static_cast<std::uint64_t>(static_cast<double>(xs.back()) * ns_per_cycle)};
}

void sender_thread(std::uint16_t port, double ns_per_cycle) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    const double interval_ns = 1e9 / kTargetRate;
    const auto interval_cycles = static_cast<std::uint64_t>(interval_ns / ns_per_cycle);
    const std::uint64_t start = cycle_now();
    for (std::uint64_t i = 0; i < kMessages; ++i) {
        const std::uint64_t due = start + i * interval_cycles;
        while (cycle_now() < due) { /* spin-pace the offered load */ }
        const auto packet = mold_add_packet(i + 1, (i % 20000) + 1);
        ::sendto(fd, packet.data(), packet.size(), 0, reinterpret_cast<sockaddr*>(&addr),
                 sizeof(addr));
    }
    const char zero = 0;  // zero-length datagram = end-of-stream sentinel
    ::sendto(fd, &zero, 0, 0, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::close(fd);
}

}  // namespace

int main() {
    const double ns_per_cycle = calibrate_ns_per_cycle();

    auto source = std::make_unique<itch::IoUringDatagramSource>("127.0.0.1", 0);
    const std::uint16_t port = source->bound_port();

    auto queue = std::make_unique<SpscQueue<PipeItem, 1u << 15>>();
    OrderBook book(1u << 20);

    std::vector<std::uint64_t> residence;
    std::vector<std::uint64_t> processing;
    std::vector<std::uint64_t> end_to_end;
    residence.reserve(kMessages);
    processing.reserve(kMessages);
    end_to_end.reserve(kMessages);

    std::thread consumer([&] {
        (void)pin_current_thread_to_core(1);
        for (;;) {
            PipeItem item;
            while (!queue->try_pop(item)) { /* spin */ }
            if (item.stop) break;
            const auto popped = cycle_now();
            book.process_event(item.event);
            const auto applied = cycle_now();
            residence.push_back(delta(popped, item.ingress_cycles));
            processing.push_back(delta(applied, popped));
            end_to_end.push_back(delta(applied, item.ingress_cycles));
        }
    });

    std::thread sender(sender_thread, port, ns_per_cycle);

    // Producer: this thread receives via io_uring, decodes MoldUDP64, timestamps, enqueues.
    (void)pin_current_thread_to_core(0);
    itch::MoldUdp64Connector connector(std::move(source));
    MarketDataEvent event{};
    const auto wall_start = std::chrono::steady_clock::now();
    while (connector.next(event)) {
        PipeItem item{event, cycle_now(), false};
        while (!queue->try_push(item)) { /* spin on backpressure */ }
    }
    PipeItem stop{};
    stop.stop = true;
    while (!queue->try_push(stop)) { /* spin */ }
    const auto wall_end = std::chrono::steady_clock::now();

    sender.join();
    consumer.join();

    const double seconds = std::chrono::duration<double>(wall_end - wall_start).count();
    const auto res = summarize(residence, ns_per_cycle);
    const auto proc = summarize(processing, ns_per_cycle);
    const auto e2e = summarize(end_to_end, ns_per_cycle);

    std::cout << "section=io_uring_pipeline"
              << " offered_messages=" << kMessages
              << " target_rate=" << static_cast<std::uint64_t>(kTargetRate)
              << " decoded=" << connector.messages_decoded()
              << " gaps=" << connector.gaps_detected()
              << " missed=" << connector.messages_missed()
              << " achieved_msgs_per_second=" << static_cast<std::uint64_t>(
                     static_cast<double>(connector.messages_decoded()) / seconds)
              << " ns_per_cycle=" << ns_per_cycle << '\n';
    std::cout << "  queue_residence_ns p50=" << res.p50 << " p95=" << res.p95
              << " p99=" << res.p99 << " max=" << res.max << '\n';
    std::cout << "  book_processing_ns p50=" << proc.p50 << " p95=" << proc.p95
              << " p99=" << proc.p99 << " max=" << proc.max << '\n';
    std::cout << "  end_to_end_ns     p50=" << e2e.p50 << " p95=" << e2e.p95
              << " p99=" << e2e.p99 << " max=" << e2e.max << '\n';

    if (connector.failed()) {
        std::cerr << "pipeline: connector reported a malformed frame\n";
        return 1;
    }
    return 0;
}
