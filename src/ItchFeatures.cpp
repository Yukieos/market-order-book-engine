#include "market/OrderBook.hpp"
#include "market/research/Features.hpp"
#include "market/itch/ItchFileConnector.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

// Phase 2 tool: replay one symbol from a real ITCH capture and emit a columnar L1
// feature stream (one row per L1 change) for the signal-validity study. The forward
// returns / IC / decay are computed by analysis/signal_validity.py from these rows.
//
//   itch_features <file> --locate N [--mold] [--out FILE] [--capacity C] [--max M]
//
// Find an active `--locate` with `itch_replay --per-symbol` (it prints most_active_symbol).
namespace {

using namespace market;

std::size_t parse_size(std::string_view text, std::size_t fallback) {
    std::size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) return fallback;
    return value;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: itch_features <file> --locate N [--mold] [--out FILE] "
                     "[--capacity C] [--max M]\n";
        return 2;
    }
    const std::string path = argv[1];
    bool mold = false;
    bool have_locate = false;
    SymbolId locate = 0;
    std::string out_path;
    std::size_t capacity = 1u << 18;
    std::size_t max_events = 0;
    for (int i = 2; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--mold") {
            mold = true;
        } else if (arg == "--locate" && i + 1 < argc) {
            locate = static_cast<SymbolId>(parse_size(argv[++i], 0));
            have_locate = true;
        } else if (arg == "--out" && i + 1 < argc) {
            out_path = argv[++i];
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = parse_size(argv[++i], capacity);
        } else if (arg == "--max" && i + 1 < argc) {
            max_events = parse_size(argv[++i], 0);
        } else {
            std::cerr << "unknown or incomplete argument: " << arg << '\n';
            return 2;
        }
    }
    if (!have_locate) {
        std::cerr << "error: --locate N is required (use itch_replay --per-symbol to find one)\n";
        return 2;
    }

    try {
        std::unique_ptr<DataConnector> connector;
        if (mold) {
            auto source = std::make_unique<itch::LengthPrefixedDatagramFileSource>(path);
            connector = std::make_unique<itch::MoldUdp64Connector>(std::move(source));
        } else {
            connector = std::make_unique<itch::ItchFileConnector>(std::filesystem::path{path});
        }

        std::ofstream file;
        if (!out_path.empty()) {
            file.open(out_path);
            if (!file) {
                std::cerr << "error: cannot open output " << out_path << '\n';
                return 1;
            }
        }
        std::ostream& out = out_path.empty() ? std::cout : file;

        OrderBook book(capacity);
        research::write_l1_header(out);
        research::L1Row previous{};
        bool have_previous = false;
        std::size_t decoded = 0;
        std::size_t rows = 0;

        MarketDataEvent event{};
        while (connector->next(event)) {
            if (event.symbol != locate) continue;  // only build the target symbol's book
            ++decoded;
            book.process_event(event);
            const auto row = research::l1_row(event.sequence, event.exchange_timestamp_ns, book);
            if (!have_previous || research::l1_changed(previous, row)) {
                research::write_l1_row(out, row);
                previous = row;
                have_previous = true;
                ++rows;
            }
            if (max_events != 0 && decoded >= max_events) break;
        }

        std::cerr << "locate=" << locate << " symbol_events=" << decoded << " rows=" << rows
                  << '\n';
    } catch (const std::exception& error) {
        std::cerr << "itch_features error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
