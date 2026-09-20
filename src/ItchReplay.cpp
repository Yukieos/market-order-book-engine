#include "market/OrderBook.hpp"
#include "market/itch/ItchFileConnector.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

// Reconstruct an order book from a real ITCH 5.0 capture and print summary
// statistics, so the decoder/connector can be validated against exchange data.
//
//   itch_replay <file> [--mold] [--capacity N] [--max N]
//
//   (default)   the file is a BinaryFILE stream: 2-byte-length-framed ITCH messages.
//   --mold      the file is length-prefixed MoldUDP64 datagrams (4-byte BE length +
//               datagram), decoded with sequence-gap tracking.
//   --capacity  order-book capacity (default 2,000,000).
//   --max       stop after N decoded messages (0 = no limit).
//
// A public sample day (e.g. NASDAQ's TotalView-ITCH 5.0 sample files) can be fed
// directly in BinaryFILE mode after decompression.
namespace {

using namespace market;

std::size_t parse_size(std::string_view text, std::size_t fallback) {
    std::size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) return fallback;
    return value;
}

struct Counts {
    std::size_t decoded{0};
    std::size_t applied{0};
    std::size_t rejected{0};
    std::size_t peak_orders{0};
};

Counts apply_all(DataConnector& connector, OrderBook& book, std::size_t max_events) {
    Counts counts;
    MarketDataEvent event{};
    while (connector.next(event)) {
        ++counts.decoded;
        if (book.process_event(event) == ProcessResult::Applied) ++counts.applied;
        else ++counts.rejected;
        if (book.order_count() > counts.peak_orders) counts.peak_orders = book.order_count();
        if (max_events != 0 && counts.decoded >= max_events) break;
    }
    return counts;
}

void print_summary(const Counts& counts, const OrderBook& book) {
    std::cout << "decoded=" << counts.decoded << " applied=" << counts.applied
              << " rejected=" << counts.rejected << " peak_orders=" << counts.peak_orders
              << " active_orders=" << book.order_count();
    if (const auto bid = book.best_bid()) {
        std::cout << " best_bid=" << bid->price_ticks << "x" << bid->total_quantity;
    } else {
        std::cout << " best_bid=none";
    }
    if (const auto ask = book.best_ask()) {
        std::cout << " best_ask=" << ask->price_ticks << "x" << ask->total_quantity;
    } else {
        std::cout << " best_ask=none";
    }
    std::cout << " state_checksum=0x" << std::hex << book.state_checksum() << std::dec << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: itch_replay <file> [--mold] [--capacity N] [--max N]\n";
        return 2;
    }
    const std::string path = argv[1];
    bool mold = false;
    std::size_t capacity = 2'000'000;
    std::size_t max_events = 0;
    for (int i = 2; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--mold") {
            mold = true;
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = parse_size(argv[++i], capacity);
        } else if (arg == "--max" && i + 1 < argc) {
            max_events = parse_size(argv[++i], 0);
        } else {
            std::cerr << "unknown or incomplete argument: " << arg << '\n';
            return 2;
        }
    }

    try {
        OrderBook book(capacity);
        if (mold) {
            auto source = std::make_unique<itch::LengthPrefixedDatagramFileSource>(path);
            itch::MoldUdp64Connector connector(std::move(source));
            const auto counts = apply_all(connector, book, max_events);
            std::cout << "source=mold gaps_detected=" << connector.gaps_detected()
                      << " messages_missed=" << connector.messages_missed()
                      << " last_sequence=" << connector.last_sequence() << ' ';
            print_summary(counts, book);
            if (connector.failed()) {
                std::cerr << "warning: stream ended on a malformed frame\n";
                return 1;
            }
        } else {
            itch::ItchFileConnector connector(std::filesystem::path{path});
            const auto counts = apply_all(connector, book, max_events);
            std::cout << "source=binaryfile ";
            print_summary(counts, book);
            if (connector.failed()) {
                std::cerr << "warning: stream ended on a malformed frame\n";
                return 1;
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "itch_replay error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
