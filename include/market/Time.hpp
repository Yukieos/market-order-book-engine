#pragma once

#include <chrono>
#include <cstdint>

// Low-overhead timing for latency measurement. cycle_now() reads a hardware cycle
// counter (x86 TSC, AArch64 virtual counter) with far less overhead than a
// steady_clock call, which matters when the quantity being measured is tens of
// nanoseconds. Cycles are converted to nanoseconds with a factor calibrated once
// against steady_clock, so no per-platform frequency register is required.
namespace market {

[[nodiscard]] inline std::uint64_t cycle_now() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    return __builtin_ia32_rdtsc();
#elif defined(__aarch64__)
    std::uint64_t value = 0;
    asm volatile("mrs %0, cntvct_el0" : "=r"(value));
    return value;
#else
    return static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Nanoseconds per cycle, measured by sampling the cycle counter across a fixed
// steady_clock interval. Call once at startup; the result is stable on a fixed-power
// machine. Returns a positive factor (falls back to 1.0 if the measurement degenerates).
[[nodiscard]] inline double calibrate_ns_per_cycle() noexcept {
    using Clock = std::chrono::steady_clock;
    constexpr auto window = std::chrono::milliseconds(20);
    const auto wall_start = Clock::now();
    const auto cycle_start = cycle_now();
    while (Clock::now() - wall_start < window) {
        // busy-wait so the cycle counter advances over a known wall interval
    }
    const auto cycle_end = cycle_now();
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             Clock::now() - wall_start)
                             .count();
    const auto cycles = cycle_end - cycle_start;
    if (cycles == 0 || wall_ns <= 0) return 1.0;
    return static_cast<double>(wall_ns) / static_cast<double>(cycles);
}

}  // namespace market
