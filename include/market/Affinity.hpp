#pragma once

namespace market {

// Pins the calling thread to a single logical core so the OS stops migrating it,
// which removes a source of latency jitter in the producer/consumer threads.
//
// Linux: implemented with pthread_setaffinity_np. Other platforms (including
// macOS/Apple Silicon, where core affinity is not exposed to user space) are
// unsupported and return false without side effects. Callers should treat a false
// return as "affinity not applied on this platform", not an error.
[[nodiscard]] bool pin_current_thread_to_core(unsigned core) noexcept;

// True when this build can apply thread affinity (i.e. compiled on Linux).
[[nodiscard]] bool affinity_supported() noexcept;

}  // namespace market
