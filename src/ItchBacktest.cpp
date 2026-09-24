#include "market/research/Backtest.hpp"
#include "market/itch/ItchFileConnector.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

// Phase 3 tool: run the leak-free, single-pass imbalance backtest on one symbol with an
// explicit decision->arrival latency, frictionless crossing fills, and a fixed-point
// deterministic ledger. See docs/research-platform.md (Phase 3).
//
//   itch_backtest <file> --locate N [--mold] [--latency-ns L] [--enter T] [--exit T]
//                        [--size Q] [--capacity C]
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
        std::cerr << "usage: itch_backtest <file> --locate N [--mold] [--latency-ns L] "
                     "[--enter T] [--exit T] [--size Q] [--capacity C]\n";
        return 2;
    }
    const std::string path = argv[1];
    bool mold = false;
    bool have_locate = false;
    research::BacktestConfig config;
    std::size_t capacity = 1u << 18;
    for (int i = 2; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--mold") {
            mold = true;
        } else if (arg == "--locate" && i + 1 < argc) {
            config.locate = static_cast<SymbolId>(parse_size(argv[++i], 0));
            have_locate = true;
        } else if (arg == "--latency-ns" && i + 1 < argc) {
            config.latency_ns = static_cast<std::uint64_t>(parse_size(argv[++i], 0));
        } else if (arg == "--enter" && i + 1 < argc) {
            config.enter_threshold = std::strtod(argv[++i], nullptr);
        } else if (arg == "--exit" && i + 1 < argc) {
            config.exit_threshold = std::strtod(argv[++i], nullptr);
        } else if (arg == "--size" && i + 1 < argc) {
            config.order_size = static_cast<Quantity>(parse_size(argv[++i], 1));
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = parse_size(argv[++i], capacity);
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

        research::Backtester backtester(config, capacity);
        MarketDataEvent event{};
        while (connector->next(event)) backtester.on_event(event);
        const auto result = backtester.finish();

        std::cout << "locate=" << config.locate << " latency_ns=" << config.latency_ns
                  << " enter=" << config.enter_threshold << " exit=" << config.exit_threshold
                  << " size=" << config.order_size << " l1_updates=" << result.l1_updates
                  << " trades=" << result.trades << " unfilled=" << result.unfilled
                  << " final_position=" << result.position
                  << " pnl_ticks=" << static_cast<double>(result.final_pnl_2x) / 2.0
                  << " checksum=0x" << std::hex << result.checksum << std::dec << '\n';
    } catch (const std::exception& error) {
        std::cerr << "itch_backtest error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
