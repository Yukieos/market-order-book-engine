#include "market/OrderBook.hpp"
#include "market/research/QueueModel.hpp"
#include "market/itch/ItchFileConnector.hpp"
#include "market/itch/MoldUdp64Connector.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

// Phase 4 study: post non-overlapping hypothetical passive limit orders at the touch on
// one symbol and measure how often, and how fast, they would fill under the MBO queue
// model. Run once per model (--multiplier 1.0 = displayed, > 1.0 = conservative) to get
// the honest displayed/conservative band (docs/research-platform.md §4.2-§4.3).
//
//   itch_queue <file> --locate N [--mold] [--side buy|sell] [--size Q]
//              [--timeout K] [--multiplier M] [--capacity C]
//
// Results are simulated fill *opportunities*, not executable live PnL: small-order,
// no-market-impact, historical-events-unchanged counterfactual.
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
        std::cerr << "usage: itch_queue <file> --locate N [--mold] [--side buy|sell] "
                     "[--size Q] [--timeout K] [--multiplier M] [--capacity C]\n";
        return 2;
    }
    const std::string path = argv[1];
    bool mold = false;
    bool have_locate = false;
    SymbolId locate = 0;
    Side side = Side::Buy;
    Quantity size = 100;
    std::size_t timeout = 1000;  // symbol events before we give up on a resting order
    double multiplier = 1.0;
    std::size_t capacity = 1u << 18;
    for (int i = 2; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--mold") {
            mold = true;
        } else if (arg == "--locate" && i + 1 < argc) {
            locate = static_cast<SymbolId>(parse_size(argv[++i], 0));
            have_locate = true;
        } else if (arg == "--side" && i + 1 < argc) {
            side = std::string_view(argv[++i]) == "sell" ? Side::Sell : Side::Buy;
        } else if (arg == "--size" && i + 1 < argc) {
            size = static_cast<Quantity>(parse_size(argv[++i], 100));
        } else if (arg == "--timeout" && i + 1 < argc) {
            timeout = parse_size(argv[++i], timeout);
        } else if (arg == "--multiplier" && i + 1 < argc) {
            multiplier = std::strtod(argv[++i], nullptr);
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = parse_size(argv[++i], capacity);
        } else {
            std::cerr << "unknown or incomplete argument: " << arg << '\n';
            return 2;
        }
    }
    if (!have_locate) {
        std::cerr << "error: --locate N is required\n";
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

        OrderBook book(capacity);
        std::optional<research::QueueModel> active;
        std::size_t events_since_post = 0;
        std::size_t posted = 0, full = 0, any = 0;
        std::vector<std::size_t> events_to_full;

        const auto resolve = [&](bool filled) {
            if (filled) {
                ++full;
                events_to_full.push_back(events_since_post);
            }
            if (active->filled() > 0) ++any;
            active.reset();
        };

        MarketDataEvent event{};
        while (connector->next(event)) {
            if (event.symbol != locate) continue;

            if (active) {
                active->on_event(event, book);  // react against the pre-event book
                ++events_since_post;
                if (active->fully_filled()) {
                    resolve(true);
                } else if (events_since_post >= timeout) {
                    resolve(false);
                }
            }

            book.process_event(event);

            if (!active) {
                const auto top = side == Side::Buy ? book.best_bid() : book.best_ask();
                if (top) {
                    auto ids = book.level_order_ids(side, top->price_ticks);
                    std::unordered_set<OrderId> cohort(ids.begin(), ids.end());
                    active.emplace(side, top->price_ticks, size, top->total_quantity, multiplier,
                                   std::move(cohort), event.exchange_timestamp_ns);
                    events_since_post = 0;
                    ++posted;
                }
            }
        }
        if (active && active->filled() > 0) ++any;  // count residual partial fill

        std::size_t median = 0;
        if (!events_to_full.empty()) {
            std::sort(events_to_full.begin(), events_to_full.end());
            median = events_to_full[events_to_full.size() / 2];
        }
        const auto rate = [&](std::size_t x) {
            return posted == 0 ? 0.0 : static_cast<double>(x) / static_cast<double>(posted);
        };
        std::cout << "locate=" << locate << " side=" << (side == Side::Buy ? "buy" : "sell")
                  << " size=" << size << " timeout=" << timeout << " multiplier=" << multiplier
                  << " posted=" << posted << " full_fills=" << full
                  << " full_fill_rate=" << rate(full) << " any_fill_rate=" << rate(any)
                  << " median_events_to_full_fill=" << median << '\n';
    } catch (const std::exception& error) {
        std::cerr << "itch_queue error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
