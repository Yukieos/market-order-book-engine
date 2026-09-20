# C++20 Market-Data Replay and Limit-Order-Book Engine

A performance-oriented Milestone 1 for deterministic market-data replay. It parses
CSV events on one thread, transfers normalized events through a bounded SPSC queue,
and applies them to a fixed-capacity L2/L3 order book on a second thread.

The book supports two modes over one shared data structure (see [ARCHITECTURE.md](ARCHITECTURE.md)):

- **Apply mode** (`process_event`) replays an exchange-decided feed (CSV or NASDAQ
  ITCH 5.0). It does not match; a crossing book is retained and exposed, because the
  feed has already been matched by the venue.
- **Match mode** (`submit`) matches an incoming order against the resting book by
  price-time priority, emits fills, and rests or kills the remainder per time-in-force.

This repository still does **not** include a strategy, risk manager, FIX session, or
exchange router. A generic TLS WebSocket connector is available as an optional target;
exchange-specific message normalization remains an injected callback rather than a core
dependency.

## Architecture

```text
CSV file or optional WSS feed
   │
   ▼
DataConnector: CSVReplayConnector / WebSocketConnector (producer thread)
   │ MarketDataEvent
   ▼
bounded SPSC ring buffer
   │ MarketDataEvent
   ▼
fixed-capacity OrderBook (consumer thread)
   ├── preallocated order and price-level pools
   ├── open-addressed order-ID and price-level indexes
   └── intrusive per-level FIFO lists (price-time priority)
```

Prices are signed integer ticks rather than floating-point values. This keeps parsing,
hashing, equality, and replay deterministic. L3 orders are held in FIFO order at each
price; L2 quantity and order count are maintained on the corresponding price level.

## Build and run

Requirements: CMake 3.20+ and a C++20 compiler.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure

./build/market_replay data/sample_events.csv 100000
./build/market_benchmark 100000
```

The benchmark executable rejects non-Release builds. The verified configuration is:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
# AppleClang Release compile command contains: -O3 -DNDEBUG
```

To compile the optional TLS WebSocket adapter, install Boost headers and OpenSSL, then:

```bash
cmake -S . -B build-websocket -DCMAKE_BUILD_TYPE=Release \
  -DMARKET_ENABLE_WEBSOCKET=ON
cmake --build build-websocket -j
```

`WebSocketConnector` follows the same `DataConnector::next()` interface as CSV replay.
It handles DNS, TLS peer/hostname verification, SNI, the WebSocket handshake, an
optional subscription frame, and clean close. A supplied `WebSocketMessageParser`
converts text frames into `MarketDataEvent`; it may return false for heartbeats and
subscription acknowledgements. The implementation follows the Boost.Beast TLS
WebSocket client model and is isolated in `market_websocket` so the default library
has no Boost/OpenSSL dependency.

CSV rows use this schema:

```text
sequence,exchange_timestamp_ns,type,order_id,side,price_ticks,quantity
```

`type` is `add`, `modify`, `cancel`, or `trade`; `side` is `buy` or `sell`.
Cancel events ignore price and quantity. Trade events identify the resting order and
decrement it by quantity, removing it on an exact fill. An overfill is rejected.

## Matching mode (`submit`)

`submit(OrderRequest, FillSink)` matches an incoming order against the resting book:

- Order types **Limit** and **Market**; time-in-force **GTC**, **IOC**, **FOK**.
- Matches best-price-first (via the price ladder) and FIFO within a level (price-time
  priority). Fills are delivered to an allocation-free `FillSink` (function pointer +
  context); the call is `noexcept` with no hot-path heap allocation.
- The remainder rests (Limit GTC), is killed (Market / IOC), or the whole order is
  rejected without side effects (FOK that cannot fully fill). `SubmitResult` encodes both
  the fill outcome and the remainder disposition.

## NASDAQ ITCH 5.0 feed handler

`itch::ItchFileConnector` implements the same `DataConnector` interface as CSV replay and
reconstructs the book in apply mode from a 2-byte-length-framed ITCH 5.0 stream. The
stateless `itch::decode_message` decoder handles the book-affecting subset — Add (`A`/`F`),
Order Executed (`E`/`C`), Order Cancel (`X`), Order Delete (`D`), Order Replace (`U`) —
at fixed big-endian offsets with no allocation. Prices are ITCH's 1/10000 integer units
mapped onto `Price` ticks. Non-book messages are skipped; a malformed or truncated frame
stops the stream and is reported by `failed()`.

## Correctness model

- Storage and both hash indexes are allocated in the `OrderBook` constructor.
- `process_event()` is `noexcept` and does not call an allocating API.
- Duplicate adds, missing modifies/cancels/trades, zero quantities, overfills, and
  capacity exhaustion return explicit `ProcessResult` values.
- A same-price modify preserves queue priority. A price or side change moves the order
  to the tail of the destination level.
- The SPSC queue reserves one slot to distinguish full from empty. Producer publication
  and consumer observation use release/acquire ordering; the reverse release/acquire
  pair protects slot reuse. Cache-line-aligned indices avoid false sharing. Compilation
  is rejected when `std::atomic<std::size_t>` is not always lock-free on the target.
- The deterministic differential test applies every event to both the optimized book
  and a simple `std::map` reference, then compares all L2 levels and ordered L3 state.
  It covers a hand-written scenario plus 20,000 seeded pseudo-random events. Every
  256-event batch also compares an order-independent 64-bit state checksum.
- A concurrent SPSC test checks ordered delivery of 100,000 values.
- Match mode is checked against an independent price-time reference matcher: a scripted
  scenario plus 20,000 randomized New/Cancel/Market/IOC/FOK operations compare the full
  fill stream and the resting book, alongside targeted price-time-priority and TIF tests.
- The ITCH decoder is checked by a round-trip differential: a minimal encoder emits a
  framed stream (A/E/C/X/D/U), which is decoded, applied, and compared against the
  `std::map` reference built from the intended events; malformed/truncated/skipped frames
  are covered.

The test suite also covers bid/ask ordering, FIFO priority, duplicate IDs, missing
cancellations, invalid quantities, partial/full trades, pool reuse after exhaustion,
and crossed-book behavior. Because this is not a matching engine, a crossing bid and
ask are deliberately retained and exposed by best-price queries.
AddressSanitizer, UndefinedBehaviorSanitizer, and ThreadSanitizer are clean on the
environment below.

## Complexity and allocation boundaries

Order-ID and level lookup use fixed-size open addressing: expected constant-time lookup,
insert, and erase, with linear worst-case probing. Linking or unlinking a known order is
constant time. Best price is now **O(1)**: each side maintains a preallocated indexed
binary heap of active price levels (a "price ladder"; max-heap for bids, min-heap for
asks), updated in O(log L) only when a level is created or emptied. This is what makes
`submit()` match through the touch efficiently, and it also makes `best_bid()`/`best_ask()`
O(1). Full `levels()`/`orders_in_priority()` snapshots still sort and remain diagnostic,
outside the hot path.

The allocation benchmark replaces scalar, array, and aligned global allocation
operators and counts calls only around the `process_event()` loop, after book and
workload initialization. A zero result supports the narrower statement **no measured
heap allocations in `process_event()` for the benchmark workload**. CSV parsing,
snapshots, setup, and benchmark result storage may allocate.

## Benchmark method

`market_benchmark` seeds 32,768 resting orders, then replays deterministic synthetic
modify workloads generated from ten different seeds. It performs one warm-up and ten
measured runs and reports the median. It reports:

- single-thread order-book throughput and measured hot-path allocations;
- match-mode `submit()` latency and allocations (single-fill marketable IOC);
- ITCH 5.0 framing+decode latency, throughput, and allocations;
- standalone CSV field parsing latency;
- successful queue-call time and total enqueue/backpressure time;
- queue residence time, order-book processing time, and end-to-end latency;
- approximate queue-depth p50/p95/p99/max sampled once per 64 events;
- a final checksum derived from every run's final order-book state.

Parsing and all consumer-side latency components are measured per event. Producer-side
enqueue and depth metrics are sampled once per 64 events to reduce observer overhead.
The open-loop producer releases batches of eight events. Before the load curve, a
binary search defines sustainable capacity as the highest rate for which median
achieved throughput is at least 98% of target and median p99 queue depth is at most 64.
The benchmark then offers 50%, 70%, 90%, and 110% of that capacity.

The externally printed checksums make the final state observable, so the compiler
cannot discard the processing loops. Different workload seeds affect those checksums;
the test suite separately proves that a state mutation changes the checksum. Allocation
counting and timing end before checksum generation, so diagnostic traversal is not
charged to the order-book hot path.

The latency measurement intentionally includes OS scheduling, and `steady_clock` calls
add observer overhead. Capacity calibration is an operational SLO definition, not a
hardware constant. Run on an otherwise idle, fixed-power machine and retain the raw
output before quoting numbers. Do not compare results across machines without recording
CPU, compiler, flags, event mix, pacing model, queue size, and book occupancy.

### Example local run

- CPU: Apple M2 Pro, 10 cores (6 performance + 4 efficiency), 16 GB memory
- OS: macOS 15.5, Darwin 24.5.0, arm64
- Compiler: AppleClang 17.0.0
- Flags: `-O3 -DNDEBUG -std=c++20 -Wall -Wextra -Wpedantic -Wconversion`
- Workload: 100,000 modify events/run, 32,768 active orders, 10 seeds/runs
- Queue: 8,191 usable slots; producer batch size 8; producer sampling interval 64
- Hot path median: **24.3M events/s**, **0 measured allocations**
- Parsing: p50 **125 ns**, p95 **166 ns**, p99 **167 ns**
- Sustainable-capacity estimate: **3.95M events/s** under the criterion above
- Aggregate final state checksum: `0xa8b3042c253b9b4d`

| Offered load | Achieved events/s | Enqueue call p99 | Queue residence p99 | Book processing p99 | End-to-end p50 / p95 / p99 | Queue depth p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 50% | 1.97M | 417 ns | 5.44 us | 167 ns | 0.395 / 2.52 / 5.50 us | 8 |
| 70% | 2.76M | 417 ns | 5.46 us | 167 ns | 0.396 / 2.42 / 5.54 us | 10 |
| 90% | 3.55M | 417 ns | 9.83 us | 166 ns | 0.437 / 2.44 / 9.90 us | 30 |
| 110% | 4.34M | 459 ns | 38.46 us | 167 ns | 0.729 / 11.98 / 38.52 us | 171 |

These are ten-run medians from one development machine, not portable performance
guarantees. The decomposition shows that the overload tail is dominated by queue
residence: order-book p99 stays near 0.17 us while 110% queue-residence p99 reaches
38.46 us.

## Current limitations

- Apply mode retains crossing orders by design (it replays a matched feed); match mode
  (`submit`) performs the matching. Self-trade is currently allowed as a documented policy.
- Capacity is fixed and an exhausted book rejects new orders.
- The ITCH connector reads the whole framed stream into memory and covers the
  book-affecting subset (A/F/E/C/X/D/U); MoldUDP64/SoupBinTCP transport, streaming reads,
  and validation against a real NASDAQ day file remain follow-ups.
- Best-price queries scan the preallocated level array; a later milestone can add a
  fixed-capacity ordered price index and benchmark the trade-off.
- Modify priority semantics are documented project semantics, not exchange-specific.
- CSV replay validates fields but does not yet enforce monotonic sequence numbers or
  exchange timestamps.
- The WebSocket transport is synchronous and generic. Production exchange support still
  needs an exchange-specific JSON/binary decoder, reconnect/backoff, authentication,
  heartbeat policy, sequence-gap recovery, and snapshot/incremental synchronization.
- Busy-wait backoff uses `std::this_thread::yield()` outside the queue; CPU affinity,
  NUMA placement, huge pages, and platform-specific pause instructions are not tuned.
- The queue has a targeted concurrency test and a clean local ThreadSanitizer run, but
  broader stress/model checking remains future work.

## Repository layout

```text
include/market/       public event, order-request, connector, queue, and order-book interfaces
include/market/itch/  NASDAQ ITCH 5.0 decoder and file/stream connector
src/                  CSV parser, ITCH connector, order book + matching, replay executable
tests/                invariant, differential, matching, ITCH round-trip, and concurrency tests
benchmarks/           order-book, matching, ITCH-decode, parsing, and pipeline benchmarks
data/                 deterministic sample replay
```
