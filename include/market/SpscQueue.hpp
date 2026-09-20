#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <new>
#include <type_traits>

namespace market {

// Bounded single-producer/single-consumer queue. One slot is reserved so full
// and empty states are distinguishable. Producer publishes data with release;
// consumer observes it with acquire. The reverse pair protects slot reuse.
template <typename T, std::size_t Capacity>
class SpscQueue {
    static_assert(Capacity >= 2, "SPSC queue needs at least two slots");
    static_assert(std::atomic<std::size_t>::is_always_lock_free,
                  "this target does not provide lock-free queue indices");
    static_assert(std::is_nothrow_copy_assignable_v<T>,
                  "queue operations are noexcept only for nothrow-assignable T");

public:
    static constexpr std::size_t usable_capacity = Capacity - 1;

    bool try_push(const T& item) noexcept {
        const auto head = head_.value.load(std::memory_order_relaxed);
        const auto next = increment(head);
        if (next == tail_.value.load(std::memory_order_acquire)) {
            return false;
        }
        storage_[head] = item;
        head_.value.store(next, std::memory_order_release);
        return true;
    }

    bool try_pop(T& item) noexcept {
        const auto tail = tail_.value.load(std::memory_order_relaxed);
        if (tail == head_.value.load(std::memory_order_acquire)) {
            return false;
        }
        item = storage_[tail];
        tail_.value.store(increment(tail), std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return tail_.value.load(std::memory_order_acquire) ==
               head_.value.load(std::memory_order_acquire);
    }

    // A racing diagnostic snapshot, not a synchronization primitive.
    [[nodiscard]] std::size_t size_approx() const noexcept {
        const auto head = head_.value.load(std::memory_order_acquire);
        const auto tail = tail_.value.load(std::memory_order_acquire);
        return head >= tail ? head - tail : Capacity - tail + head;
    }

private:
    struct alignas(64) PaddedIndex {
        std::atomic<std::size_t> value{0};
    };

    static constexpr std::size_t increment(std::size_t index) noexcept {
        return (index + 1) % Capacity;
    }

    std::array<T, Capacity> storage_{};
    PaddedIndex head_{};
    PaddedIndex tail_{};
};

}  // namespace market
