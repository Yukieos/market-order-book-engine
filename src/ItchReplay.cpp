#include "market/MultiSymbolBook.hpp"
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

Counts apply_all_symbols(DataConnector& connector, MultiSymbolBook& books, std::size_t max_events) {
    Counts counts;
    MarketDataEvent event{};
    while (connector.next(event)) {
        ++counts.decoded;
        if (books.process_event(event) == ProcessResult::Applied) ++counts.applied;
        else ++counts.rejected;
        if (max_events != 0 && counts.decoded >= max_events) break;
    }
    return counts;
}

void print_symbol_summary(const Counts& counts, const MultiSymbolBook& books) {
    SymbolId best_symbol = 0;
    std::size_t best_orders = 0;
    for (const SymbolId symbol : books.symbols()) {
        const std::size_t n = books.book(symbol)->order_count();
        if (n > best_orders) {
            best_orders = n;
            best_symbol = symbol;
        }
    }
    std::cout << "decoded=" << counts.decoded << " applied=" << counts.applied
              << " rejected=" << counts.rejected << " symbols=" << books.symbol_count()
              << " total_orders=" << books.total_orders()
              << " most_active_symbol=" << best_symbol << " orders=" << best_orders;
    if (const auto bid = books.best_bid(best_symbol)) {
        std::cout << " best_bid=" << bid->price_ticks << "x" << bid->total_quantity;
    } else {
        std::cout << " best_bid=none";
    }
    if (const auto ask = books.best_ask(best_symbol)) {
        std::cout << " best_ask=" << ask->price_ticks << "x" << ask->total_quantity;
    } else {
        std::cout << " best_ask=none";
    }
    std::cout << " state_checksum=0x" << std::hex << books.state_checksum() << std::dec << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: itch_replay <file> [--mold] [--per-symbol] "
                     "[--capacity N] [--per-symbol-capacity N] [--max N]\n";
        return 2;
    }
    const std::string path = argv[1];
    bool mold = false;
    bool per_symbol = false;
    std::size_t capacity = 2'000'000;
    std::size_t per_symbol_capacity = 8'192;
    std::size_t max_events = 0;
    for (int i = 2; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--mold") {
            mold = true;
        } else if (arg == "--per-symbol") {
            per_symbol = true;
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = parse_size(argv[++i], capacity);
        } else if (arg == "--per-symbol-capacity" && i + 1 < argc) {
            per_symbol_capacity = parse_size(argv[++i], per_symbol_capacity);
        } else if (arg == "--max" && i + 1 < argc) {
            max_events = parse_size(argv[++i], 0);
        } else {
            std::cerr << "unknown or incomplete argument: " << arg << '\n';
            return 2;
        }
    }

    try {
        std::unique_ptr<DataConnector> connector;
        std::string label;
        if (mold) {
            auto source = std::make_unique<itch::LengthPrefixedDatagramFileSource>(path);
            connector = std::make_unique<itch::MoldUdp64Connector>(std::move(source));
            label = "source=mold";
        } else {
            connector = std::make_unique<itch::ItchFileConnector>(std::filesystem::path{path});
            label = "source=binaryfile";
        }

        bool failed = false;
        if (per_symbol) {
            MultiSymbolBook books(per_symbol_capacity);
            const auto counts = apply_all_symbols(*connector, books, max_events);
            std::cout << label << " mode=per-symbol per_symbol_capacity=" << per_symbol_capacity
                      << ' ';
            print_symbol_summary(counts, books);
        } else {
            OrderBook book(capacity);
            const auto counts = apply_all(*connector, book, max_events);
            std::cout << label << ' ';
            print_summary(counts, book);
        }

        if (mold) {
            auto* mold_connector = static_cast<itch::MoldUdp64Connector*>(connector.get());
            std::cout << "mold_stats gaps_detected=" << mold_connector->gaps_detected()
                      << " messages_missed=" << mold_connector->messages_missed()
                      << " last_sequence=" << mold_connector->last_sequence() << '\n';
            failed = mold_connector->failed();
        } else {
            failed = static_cast<itch::ItchFileConnector*>(connector.get())->failed();
        }
        if (failed) {
            std::cerr << "warning: stream ended on a malformed frame\n";
            return 1;
        }
    } catch (const std::exception& error) {
        std::cerr << "itch_replay error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
