#include "market/CSVReplayConnector.hpp"
#include "market/OrderBook.hpp"
#include "market/SpscQueue.hpp"
#include "market/StateChecksum.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <new>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifndef MARKET_BENCHMARK_FLAGS
#define MARKET_BENCHMARK_FLAGS "unknown"
#endif

namespace allocation_counter {
std::atomic<std::uint64_t> calls{0};
std::atomic<bool> enabled{false};

void record() noexcept {
    if (enabled.load(std::memory_order_relaxed)) calls.fetch_add(1, std::memory_order_relaxed);
}
}  // namespace allocation_counter

void* operator new(std::size_t size) {
    if (void* pointer = std::malloc(std::max<std::size_t>(size, 1))) {
        allocation_counter::record();
        return pointer;
    }
    throw std::bad_alloc();
}

void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { std::free(pointer); }

void* operator new(std::size_t size, std::align_val_t alignment) {
    void* pointer = nullptr;
    if (posix_memalign(&pointer, static_cast<std::size_t>(alignment), std::max<std::size_t>(size, 1)) != 0) {
        throw std::bad_alloc();
    }
    allocation_counter::record();
    return pointer;
}
void operator delete(void* pointer, std::align_val_t) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept { std::free(pointer); }
void* operator new[](std::size_t size, std::align_val_t alignment) { return ::operator new(size, alignment); }
void operator delete[](void* pointer, std::align_val_t) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept { std::free(pointer); }

namespace {

using Clock = std::chrono::steady_clock;
using namespace market;

constexpr std::size_t measured_runs = 10;
constexpr std::size_t active_orders = 32'768;
constexpr std::uint64_t base_seed = 0x5eed'1234'9876'abcdULL;

std::uint64_t now_ns() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

std::size_t parse_count(std::string_view text) {
    std::size_t value{};
    const auto [ptr, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || ptr != text.data() + text.size() || value == 0) {
        throw std::invalid_argument("event count must be a positive integer");
    }
    return value;
}

struct Distribution {
    std::uint64_t p50{};
    std::uint64_t p95{};
    std::uint64_t p99{};
    std::uint64_t maximum{};
};

Distribution summarize(std::vector<std::uint64_t>& samples) {
    std::sort(samples.begin(), samples.end());
    const auto at = [&](double fraction) {
        return samples[static_cast<std::size_t>(fraction * static_cast<double>(samples.size() - 1))];
    };
    return {at(0.50), at(0.95), at(0.99), samples.back()};
}

template <typename T>
T median(std::vector<T> values) {
    std::sort(values.begin(), values.end());
    const auto upper = values.size() / 2;
    if (values.size() % 2 != 0) return values[upper];
    return static_cast<T>((static_cast<long double>(values[upper - 1]) +
                           static_cast<long double>(values[upper])) /
                          2.0L);
}

std::vector<MarketDataEvent> make_workload(std::size_t count, std::uint64_t seed) {
    std::mt19937_64 random(seed);
    std::vector<MarketDataEvent> events;
    events.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const auto id = static_cast<OrderId>((random() % active_orders) + 1);
        events.push_back({.sequence = i + 1,
                          .exchange_timestamp_ns = i * 100,
                          .type = EventType::Modify,
                          .order_id = id,
                          .side = random() % 2 == 0 ? Side::Buy : Side::Sell,
                          .price_ticks = static_cast<Price>(9'936 + random() % 128),
                          .quantity = static_cast<Quantity>(1 + random() % 100)});
    }
    return events;
}

void seed_book(OrderBook& book) {
    for (std::size_t i = 0; i < active_orders; ++i) {
        const auto id = static_cast<OrderId>(i + 1);
        const MarketDataEvent input{.type = EventType::Add,
                                    .order_id = id,
                                    .side = id % 2 == 0 ? Side::Buy : Side::Sell,
                                    .price_ticks = static_cast<Price>(9'936 + (id % 128)),
                                    .quantity = 10};
        if (book.process_event(input) != ProcessResult::Applied) throw std::runtime_error("seed failed");
    }
}

struct HotRun {
    double events_per_second{};
    std::uint64_t allocations{};
    std::uint64_t checksum{};
};

HotRun run_hot_path(const std::vector<MarketDataEvent>& events) {
    OrderBook book(active_orders * 2);
    seed_book(book);
    allocation_counter::calls.store(0, std::memory_order_relaxed);
    allocation_counter::enabled.store(true, std::memory_order_release);
    const auto start = Clock::now();
    for (const auto& input : events) {
        if (book.process_event(input) != ProcessResult::Applied) throw std::runtime_error("hot-path event rejected");
    }
    const auto end = Clock::now();
    allocation_counter::enabled.store(false, std::memory_order_release);
    const auto checksum = book.state_checksum();
    std::atomic_signal_fence(std::memory_order_seq_cst);
    const auto seconds = std::chrono::duration<double>(end - start).count();
    return {static_cast<double>(events.size()) / seconds, allocation_counter::calls.load(), checksum};
}

std::vector<std::string> make_csv_lines(std::size_t count, std::uint64_t seed) {
    const auto events = make_workload(count, seed);
    std::vector<std::string> lines;
    lines.reserve(events.size());
    for (const auto& event : events) {
        lines.push_back(std::to_string(event.sequence) + ',' + std::to_string(event.exchange_timestamp_ns) +
                        ",modify," + std::to_string(event.order_id) + ',' +
                        (event.side == Side::Buy ? "buy," : "sell,") + std::to_string(event.price_ticks) + ',' +
                        std::to_string(event.quantity));
    }
    return lines;
}

struct ParseRun {
    Distribution latency;
    std::uint64_t checksum{};
};

ParseRun run_parsing(const std::vector<std::string>& lines) {
    std::vector<std::uint64_t> latency(lines.size());
    std::uint64_t checksum = 0;
    for (std::size_t i = 0; i < lines.size(); ++i) {
        const auto begin = now_ns();
        const auto event = parse_csv_event(lines[i]);
        const auto end = now_ns();
        latency[i] = end - begin;
        checksum ^= checksum_mix(event.order_id ^ static_cast<std::uint64_t>(event.price_ticks) ^ event.quantity);
    }
    return {summarize(latency), checksum};
}

struct TimedEvent {
    MarketDataEvent event{};
    std::uint64_t ready_ns{};
    std::uint64_t publish_start_ns{};
    bool sampled{};
};

struct PipelineRun {
    double events_per_second{};
    Distribution enqueue_call;
    Distribution enqueue_total;
    Distribution queue_residence;
    Distribution processing;
    Distribution end_to_end;
    Distribution queue_depth;
    std::uint64_t checksum{};
};

PipelineRun run_pipeline(const std::vector<MarketDataEvent>& events, double target_rate) {
    constexpr std::size_t queue_slots = 8192;
    constexpr std::size_t pacing_batch_size = 8;
    constexpr std::size_t producer_sample_interval = 64;
    auto queue = std::make_unique<SpscQueue<TimedEvent, queue_slots>>();
    OrderBook book(active_orders * 2);
    seed_book(book);
    std::vector<std::uint64_t> enqueue_call;
    std::vector<std::uint64_t> enqueue_total;
    std::vector<std::uint64_t> queue_residence(events.size());
    std::vector<std::uint64_t> processing(events.size());
    std::vector<std::uint64_t> end_to_end(events.size());
    std::vector<std::uint64_t> queue_depth;
    enqueue_call.reserve(events.size() / producer_sample_interval + 1);
    enqueue_total.reserve(events.size() / producer_sample_interval + 1);
    queue_depth.reserve(events.size() / producer_sample_interval + 1);
    std::atomic<bool> start{false};
    std::atomic<std::uint64_t> start_ns{0};
    std::atomic<bool> rejected{false};

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        const auto epoch = start_ns.load(std::memory_order_acquire);
        for (std::size_t batch = 0; batch < events.size(); batch += pacing_batch_size) {
            if (target_rate > 0.0) {
                const auto release = epoch + static_cast<std::uint64_t>(
                    static_cast<double>(batch) * 1'000'000'000.0 / target_rate);
                while (now_ns() < release) std::atomic_signal_fence(std::memory_order_seq_cst);
            }
            const auto batch_end = std::min(events.size(), batch + pacing_batch_size);
            for (std::size_t i = batch; i < batch_end; ++i) {
                const auto ready = now_ns();
                const bool sample = i % producer_sample_interval == 0;
                TimedEvent timed{.event = events[i],
                                 .ready_ns = ready,
                                 .publish_start_ns = ready,
                                 .sampled = sample};
                for (;;) {
                    if (sample) timed.publish_start_ns = now_ns();
                    if (queue->try_push(timed)) {
                        if (sample) {
                            const auto published = now_ns();
                            enqueue_call.push_back(published - timed.publish_start_ns);
                            enqueue_total.push_back(published - timed.ready_ns);
                            queue_depth.push_back(queue->size_approx());
                        }
                        break;
                    }
                    timed.publish_start_ns = now_ns();
                    std::this_thread::yield();
                }
            }
        }
    });

    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (std::size_t i = 0; i < events.size(); ++i) {
            TimedEvent timed;
            while (!queue->try_pop(timed)) std::this_thread::yield();
            const auto dequeued = now_ns();
            if (book.process_event(timed.event) != ProcessResult::Applied) {
                rejected.store(true, std::memory_order_relaxed);
            }
            const auto process_end = now_ns();
            queue_residence[i] = dequeued - timed.publish_start_ns;
            processing[i] = process_end - dequeued;
            end_to_end[i] = process_end - timed.ready_ns;
        }
    });

    const auto begin = now_ns();
    start_ns.store(begin, std::memory_order_release);
    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();
    const auto end = now_ns();
    if (rejected.load(std::memory_order_relaxed)) throw std::runtime_error("pipeline event rejected");
    return {
        static_cast<double>(events.size()) * 1'000'000'000.0 / static_cast<double>(end - begin),
        summarize(enqueue_call),
        summarize(enqueue_total),
        summarize(queue_residence),
        summarize(processing),
        summarize(end_to_end),
        summarize(queue_depth),
        book.state_checksum(),
    };
}

Distribution median_distribution(const std::vector<PipelineRun>& runs,
                                 Distribution PipelineRun::*member) {
    std::vector<std::uint64_t> p50;
    std::vector<std::uint64_t> p95;
    std::vector<std::uint64_t> p99;
    std::vector<std::uint64_t> maximum;
    for (const auto& run : runs) {
        p50.push_back((run.*member).p50);
        p95.push_back((run.*member).p95);
        p99.push_back((run.*member).p99);
        maximum.push_back((run.*member).maximum);
    }
    return {median(p50), median(p95), median(p99), median(maximum)};
}

void print_distribution(std::string_view name, const Distribution& value, std::string_view unit = "ns") {
    std::cout << ' ' << name << "_p50_" << unit << '=' << value.p50
              << ' ' << name << "_p95_" << unit << '=' << value.p95
              << ' ' << name << "_p99_" << unit << '=' << value.p99
              << ' ' << name << "_max_" << unit << '=' << value.maximum;
}

std::uint64_t fold_checksum(std::uint64_t aggregate, std::uint64_t value, std::uint64_t seed) {
    return checksum_mix(aggregate ^ std::rotl(value, static_cast<int>(seed & 63U)) ^ seed);
}

void benchmark_hot_path(std::size_t count) {
    (void)run_hot_path(make_workload(std::min<std::size_t>(count, 20'000), base_seed - 1));
    std::vector<double> throughput;
    std::uint64_t maximum_allocations = 0;
    std::uint64_t checksum = 0;
    for (std::size_t run = 0; run < measured_runs; ++run) {
        const auto seed = base_seed + run;
        const auto result = run_hot_path(make_workload(count, seed));
        throughput.push_back(result.events_per_second);
        maximum_allocations = std::max(maximum_allocations, result.allocations);
        checksum = fold_checksum(checksum, result.checksum, seed);
    }
    std::cout << "section=hot_path runs=" << measured_runs
              << " events_per_run=" << count
              << " median_events_per_second=" << static_cast<std::uint64_t>(median(throughput))
              << " max_measured_heap_allocations=" << maximum_allocations
              << " final_checksum=0x" << std::hex << checksum << std::dec << '\n';
}

void benchmark_parsing(std::size_t count) {
    (void)run_parsing(make_csv_lines(std::min<std::size_t>(count, 10'000), base_seed - 1));
    std::vector<std::uint64_t> p50;
    std::vector<std::uint64_t> p95;
    std::vector<std::uint64_t> p99;
    std::vector<std::uint64_t> maximum;
    std::uint64_t checksum = 0;
    for (std::size_t run = 0; run < measured_runs; ++run) {
        const auto seed = base_seed + run;
        const auto result = run_parsing(make_csv_lines(count, seed));
        p50.push_back(result.latency.p50);
        p95.push_back(result.latency.p95);
        p99.push_back(result.latency.p99);
        maximum.push_back(result.latency.maximum);
        checksum = fold_checksum(checksum, result.checksum, seed);
    }
    std::cout << "section=parsing runs=" << measured_runs << " events_per_run=" << count;
    print_distribution("parse", {median(p50), median(p95), median(p99), median(maximum)});
    std::cout << " final_checksum=0x" << std::hex << checksum << std::dec << '\n';
}

double calibrate_pipeline(std::size_t count) {
    constexpr std::size_t calibration_repetitions = 3;
    constexpr std::size_t search_steps = 8;
    constexpr std::uint64_t maximum_p99_queue_depth = 64;
    const auto calibration_count = std::max<std::size_t>(30'000, std::min<std::size_t>(count, 100'000));
    (void)run_pipeline(make_workload(20'000, base_seed - 2), 0.0);
    std::vector<double> unthrottled;
    for (std::size_t run = 0; run < calibration_repetitions; ++run) {
        unthrottled.push_back(run_pipeline(make_workload(calibration_count, base_seed - 10 - run), 0.0)
                                  .events_per_second);
    }
    double low = 0.0;
    double high = median(unthrottled) * 1.10;
    for (std::size_t step = 0; step < search_steps; ++step) {
        const auto candidate = (low + high) / 2.0;
        std::vector<double> achieved;
        std::vector<std::uint64_t> p99_depth;
        for (std::size_t run = 0; run < calibration_repetitions; ++run) {
            const auto result = run_pipeline(
                make_workload(calibration_count, base_seed - 100 - step * calibration_repetitions - run),
                candidate);
            achieved.push_back(result.events_per_second);
            p99_depth.push_back(result.queue_depth.p99);
        }
        const bool sustainable = median(achieved) >= candidate * 0.98 &&
                                 median(p99_depth) <= maximum_p99_queue_depth;
        if (sustainable) low = candidate;
        else high = candidate;
    }
    return low;
}

void benchmark_load_levels(std::size_t count) {
    const auto capacity = calibrate_pipeline(count);
    std::cout << "section=calibration sustainable_events_per_second="
              << static_cast<std::uint64_t>(capacity)
              << " criterion=\"achieved>=98%_target_and_p99_queue_depth<=64\""
              << " search_steps=8 repetitions_per_step=3\n";
    constexpr std::array<double, 4> loads{0.50, 0.70, 0.90, 1.10};
    for (const auto load : loads) {
        std::vector<PipelineRun> runs;
        runs.reserve(measured_runs);
        std::uint64_t checksum = 0;
        for (std::size_t run = 0; run < measured_runs; ++run) {
            const auto seed = base_seed + run;
            runs.push_back(run_pipeline(make_workload(count, seed), capacity * load));
            checksum = fold_checksum(checksum, runs.back().checksum, seed);
        }
        std::vector<double> throughput;
        for (const auto& run : runs) throughput.push_back(run.events_per_second);
        std::cout << std::fixed << std::setprecision(2)
                  << "section=pipeline load_percent=" << load * 100.0
                  << " target_events_per_second=" << static_cast<std::uint64_t>(capacity * load)
                  << " median_achieved_events_per_second=" << static_cast<std::uint64_t>(median(throughput));
        print_distribution("enqueue_call", median_distribution(runs, &PipelineRun::enqueue_call));
        print_distribution("enqueue_total", median_distribution(runs, &PipelineRun::enqueue_total));
        print_distribution("queue_residence", median_distribution(runs, &PipelineRun::queue_residence));
        print_distribution("processing", median_distribution(runs, &PipelineRun::processing));
        print_distribution("end_to_end", median_distribution(runs, &PipelineRun::end_to_end));
        print_distribution("queue_depth", median_distribution(runs, &PipelineRun::queue_depth), "events");
        std::cout << " final_checksum=0x" << std::hex << checksum << std::dec << '\n';
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
#ifndef NDEBUG
        throw std::runtime_error("benchmarks require a Release build with -DNDEBUG");
#endif
        const auto count = argc > 1 ? parse_count(argv[1]) : 100'000;
        std::cout << "benchmark_flags=\"" << MARKET_BENCHMARK_FLAGS << "\" events=" << count
                  << " measured_runs=" << measured_runs << " warmup_runs=1 seeds=" << measured_runs << '\n';
        benchmark_hot_path(count);
        benchmark_parsing(count);
        benchmark_load_levels(count);
    } catch (const std::exception& error) {
        allocation_counter::enabled.store(false, std::memory_order_relaxed);
        std::cerr << "benchmark error: " << error.what() << '\n';
        return 1;
    }
}
