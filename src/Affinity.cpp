#include "market/Affinity.hpp"

#if defined(__linux__)
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <pthread.h>
#include <sched.h>
#endif

namespace market {

bool affinity_supported() noexcept {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

bool pin_current_thread_to_core(unsigned core) noexcept {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(core), &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)core;
    return false;  // affinity not exposed to user space on this platform (e.g. macOS)
#endif
}

}  // namespace market
