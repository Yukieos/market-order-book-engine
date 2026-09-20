#include "market/CSVReplayConnector.hpp"
#include "market/OrderBook.hpp"
#include "market/SpscQueue.hpp"

#include <atomic>
#include <charconv>
#include <exception>
#include <iostream>
#include <memory>
#include <string_view>
#include <thread>

namespace {

std::size_t parse_capacity(std::string_view text) {
    std::size_t value{};
    const auto [ptr, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || ptr != text.data() + text.size() || value == 0) {
        throw std::invalid_argument("capacity must be a positive integer");
    }
    return value;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "usage: market_replay <events.csv> [order-capacity]\n";
        return 2;
    }

    try {
        constexpr std::size_t queue_slots = 65'536;
        const auto capacity = argc == 3 ? parse_capacity(argv[2]) : 100'000;
        auto queue = std::make_unique<market::SpscQueue<market::MarketDataEvent, queue_slots>>();
        market::OrderBook book(capacity);
        std::atomic<bool> producer_done{false};
        std::exception_ptr producer_error;
        std::size_t applied = 0;
        std::size_t rejected = 0;

        std::thread producer([&] {
            try {
                market::CSVReplayConnector connector(argv[1]);
                market::MarketDataEvent event;
                while (connector.next(event)) {
                    while (!queue->try_push(event)) std::this_thread::yield();
                }
            } catch (...) {
                producer_error = std::current_exception();
            }
            producer_done.store(true, std::memory_order_release);
        });

        std::thread consumer([&] {
            market::MarketDataEvent event;
            for (;;) {
                if (queue->try_pop(event)) {
                    if (book.process_event(event) == market::ProcessResult::Applied) ++applied;
                    else ++rejected;
                } else if (producer_done.load(std::memory_order_acquire)) {
                    break;
                } else {
                    std::this_thread::yield();
                }
            }
        });

        producer.join();
        consumer.join();
        if (producer_error) std::rethrow_exception(producer_error);

        std::cout << "applied=" << applied << " rejected=" << rejected
                  << " active_orders=" << book.order_count();
        if (const auto bid = book.best_bid()) std::cout << " best_bid=" << bid->price_ticks;
        if (const auto ask = book.best_ask()) std::cout << " best_ask=" << ask->price_ticks;
        std::cout << '\n';
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}

