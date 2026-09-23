# Architecture (target for M2 + M3)

_Last updated: 2026-09-20_

This document is the shared mental model for the next two milestones:

- **M2 — Matching engine core** (the book decides matches for incoming orders).
- **M3 — NASDAQ ITCH 5.0 feed handler** (reconstruct a book from a real exchange feed).

It builds on Milestone 1 ([README.md](README.md)) and refines that design where the
new work demands it. Milestone 1's spine is unchanged: **producer thread → lock-free
SPSC ring buffer → single-threaded book on the consumer thread**, integer tick prices,
preallocated pools, `noexcept` allocation-free hot path, differential oracle + sanitizers
+ reproducible benchmark as acceptance gates.

---

## 1. The core insight: two modes, one book

A market venue and a market-data replay do *opposite* jobs, and conflating them is the
mistake most hobby order books make. We keep them explicit:

| Mode | Who decides matches | Input | This project |
|---|---|---|---|
| **Apply / reconstruct** | The exchange already did | A *matched* feed (CSV, **ITCH**) | Milestone 1 + **M3** |
| **Match / simulate** | *Our* book does | Client **order requests** | **M2** |

Both modes share one `OrderBook` data structure (pools, indexes, intrusive FIFO lists,
price ladder). They differ only in the entry point:

- `process_event(MarketDataEvent)` — **apply** an exchange-decided event. No matching;
  a crossing book is retained and exposed (Milestone 1 behavior). ITCH uses this.
- `submit(OrderRequest, FillSink)` — **match** an incoming order against the resting
  book by price-time priority, emit fills, rest/kill the remainder. New in M2.

This separation is why ITCH (an already-matched feed) and the matching engine can live
in one repo without either corrupting the other's semantics.

```text
        ┌──────────────────────── producer thread ───────────────────────┐
CSV  ─┐  │                                                                 │
ITCH ─┼─▶│  DataConnector::next()  ──▶  MarketDataEvent  ──▶ SPSC ring ─────┼─┐
WSS  ─┘  │  (CSV / ITCH decoder / WebSocket)                               │ │
        └─────────────────────────────────────────────────────────────────┘ │
                                                                              ▼
        ┌──────────────────────── consumer thread ──────────────────────────────┐
        │  OrderBook                                                             │
        │    • apply mode:  process_event(MarketDataEvent)   ← CSV / ITCH        │
        │    • match mode:  submit(OrderRequest, FillSink)   ← M2 client orders  │
        │    • shared core: order pool · level pool · id index · level index     │
        │                   · intrusive price-time FIFO · price ladder (best px) │
        └────────────────────────────────────────────────────────────────────────┘
                    │ apply: book state                    │ match: Fill stream
                    ▼                                        ▼
             best_bid/ask, levels()                    FillSink (callback)
```

---

## 2. Shared core changes (needed by both M2 and M3)

### 2.1 Efficient best price — the "price ladder"

Milestone 1's `best_bid()`/`best_ask()` **scan the level array (O(levels))**. Matching
walks the touch repeatedly, so an O(L) best-price query would dominate the hot path.

**Change:** maintain, per side, an **indexed binary heap of active level slots keyed by
price** (max-heap for bids, min-heap for asks). A price level's price is fixed for its
lifetime, so the heap only needs:

- push on `acquire_level()` — O(log L),
- remove-arbitrary on `release_level()` (usually the top after it's consumed) — O(log L),
- peek top for best price — **O(1)**.

Each `Level` stores its position in its side's heap so arbitrary removal is O(log L). The
heap arrays are preallocated (capacity = `max_orders`), so this stays allocation-free.
This also upgrades `best_bid()`/`best_ask()` to O(1) — an early down-payment on the
roadmap's M5 "O(1) best price" item. (A hierarchical bitmap ladder for hard-real-time
O(1) across a bounded tick domain remains the M5 upgrade path.)

### 2.2 Event model extensions

`MarketDataEvent` gains what a real feed needs (all additive, still POD / nothrow-copyable):

```cpp
enum class EventType : uint8_t { Add, Modify, Cancel, Trade, Reduce, Replace };
//                                                          ^^^^^^  ^^^^^^^ new
struct MarketDataEvent {
    ...
    OrderId  order_id{};        // for Replace: the ORIGINAL order ref
    OrderId  new_order_id{};    // NEW — only meaningful for Replace
    ...
};
```

- **Reduce** — decrement a resting order by N shares, remove on reaching 0 (partial
  cancel). Same mechanic as `Trade` but *not* a trade print (kept distinct so trade-volume
  analytics stay honest). ITCH `X` maps here.
- **Replace** — atomic cancel-orig + add-new. The message carries no side, so the book
  looks up the original order's side by `order_id`, removes it, and inserts `new_order_id`
  at the new price/qty with the inherited side. ITCH `U` maps here.

`process_event()` gains `Reduce` and `Replace` branches, both reusing existing private
helpers (`unlink`, `link_tail`, `acquire_level`, lookup insert/erase).

---

## 3. M2 — Matching engine

### 3.1 New types (`include/market/OrderTypes.hpp`)

```cpp
enum class OrderType   : uint8_t { Limit, Market };
enum class TimeInForce : uint8_t { GTC, IOC, FOK };
enum class RequestType : uint8_t { New, Cancel };   // Modify can follow later

struct OrderRequest {
    RequestType   type{};
    OrderId       id{};            // client order id (taker id when New)
    Side          side{};
    Price         price_ticks{};   // ignored for Market
    Quantity      quantity{};
    OrderType     order_type{};
    TimeInForce   tif{};
};

struct Fill {
    OrderId   taker_id{};
    OrderId   maker_id{};
    Side      taker_side{};
    Price     price_ticks{};       // maker's resting price (price-time priority)
    Quantity  quantity{};
    uint64_t  sequence{};          // deterministic fill ordering
};

// Allocation-free, cross-TU, noexcept output sink (function ptr + context).
struct FillSink {
    void* ctx{};
    void (*fn)(void*, const Fill&) noexcept {};
    void operator()(const Fill& f) const noexcept { if (fn) fn(ctx, f); }
};

enum class SubmitResult : uint8_t {
    Resting, FilledComplete, FilledPartialResting, FilledPartialKilled,
    RejectedFOK, RejectedDuplicate, RejectedCapacity, RejectedInvalid
};
```

### 3.2 Entry point

```cpp
SubmitResult OrderBook::submit(const OrderRequest& req, const FillSink& sink) noexcept;
```

Matching lives **inside `OrderBook`** to reuse its private pool/index/ladder helpers
(rather than duplicating them in a separate class). "Matching engine" is therefore the
`submit()` code path, not a second data structure.

### 3.3 Matching algorithm (New order)

```
remaining = req.quantity
while remaining > 0 and opposite side has a best level that this order crosses:
    level = best opposite level (price ladder top)
    walk level's FIFO from head (oldest maker first):
        maker = head order
        traded = min(remaining, maker.quantity)
        emit Fill{ taker=req.id, maker=maker.id, price=level.price, qty=traded, seq++ }
        maker.quantity -= traded ; level.total -= traded ; remaining -= traded
        if maker.quantity == 0: unlink+free maker (remove level if empty → ladder pop)
        if remaining == 0: break
crossing test:  buy crosses when req.price >= ask.price ; sell when req.price <= bid.price
                Market orders cross any level (ignore price).
after the loop:
    Limit + GTC   : rest `remaining` as a new resting order (reuse add path)
    Limit + IOC   : drop remainder (no rest)
    Market        : drop remainder (never rests)
    FOK           : pre-scan for full fill; if impossible, emit nothing and reject
```

Self-trade handling is a documented policy (default: allowed, like Milestone 1's retained
crossing book) — noted, not silently assumed. The hot path allocates nothing: fills go to
the caller's `FillSink`; no container growth.

### 3.4 Correctness (acceptance gate)

- **Reference matcher oracle:** a straightforward `std::map<price, deque<order>>` matcher
  in the test TU. Feed identical request streams to both; assert **identical fill streams**
  (price, qty, maker/taker order) and **identical final book** (extends Milestone 1's
  differential harness).
- **Property tests:** conservation of shares (Σ fills + resting = submitted), price-time
  fairness (older maker at a price fills first), no trade at a worse price than the taker's
  limit, FOK is all-or-nothing, IOC never rests.
- **Fuzz** interleaved New/Cancel/Market/IOC/FOK sequences; assert invariants + oracle.
- **Sanitizers** ASan/UBSan/TSan clean; **submit()** measured at **0 hot-path allocations**.
- **Benchmark:** match latency p50/p99 (HdrHistogram-style) on a marketable-flow workload.

---

## 4. M3 — NASDAQ ITCH 5.0 feed handler

Goal: turn a real exchange feed into `MarketDataEvent`s that flow through the *existing*
SPSC → book pipeline in **apply mode**, and reconstruct the displayed L2/L3 book.

### 4.1 Framing & decoding

- **Framing:** NASDAQ BinaryFILE / MoldUDP64 payloads prefix each ITCH message with a
  **2-byte big-endian length**. The connector reads length, then that many bytes.
- **Decoder** (`include/market/itch/ItchDecoder.hpp`): a **stateless, zero-allocation**
  function over a byte span → `std::optional<MarketDataEvent>`. Fields are read at fixed
  big-endian offsets (no struct punning across alignment/endianness). Unknown/irrelevant
  message types return `nullopt` (skipped), like heartbeat handling in the WSS parser.
- **Prices:** ITCH price is a 4-byte unsigned integer in units of $0.0001 → stored
  directly as `price_ticks` (tick = 1/10000). **Timestamps:** 6-byte ns-since-midnight.
  **Order refs:** 8-byte → `OrderId`. **Shares:** 4-byte → `Quantity`.

### 4.2 Message → event mapping (book-affecting subset)

| ITCH | Meaning | → `MarketDataEvent` |
|---|---|---|
| `A` / `F` | Add order (with/without MPID) | `Add` (side, price, shares, ref) |
| `E` | Order executed | `Trade` (reduce ref by executed shares) |
| `C` | Order executed with price | `Trade` (reduce; carries print price) |
| `X` | Order cancel (partial) | `Reduce` (ref by canceled shares) |
| `D` | Order delete (full) | `Cancel` (ref) |
| `U` | Order replace | `Replace` (orig ref → new ref, new px/qty) |
| `S`,`R`,`P`,`Q`,`I`,… | system/directory/trade/NOII | skipped for book build (used later for analytics) |

`U` is why the book handles Replace internally (§2.2): the message has no side, so the
book inherits it from the original order.

### 4.3 Connector (`include/market/itch/ItchFileConnector.hpp` + `src/ItchFileConnector.cpp`)

Implements the existing `DataConnector` interface so it drops into
[Main.cpp](src/Main.cpp) unchanged:

```cpp
class ItchFileConnector : public DataConnector {
    bool next(MarketDataEvent& out) noexcept override;  // one event per call
};
```

`Replace` is emitted as a single `Replace` event (the book expands it). The connector
holds no per-order state; it owns only a buffered read window over the file/stream.

### 4.4 Correctness (acceptance gate)

Real NASDAQ day files are large and licensed, so today's rigor comes from a
**round-trip differential test** (no external data needed):

1. A tiny **ITCH encoder** in the test emits a byte stream from a scripted event list
   (A/F/E/C/X/D/U), including the 2-byte length framing and edge cases (full fill via E,
   partial cancel via X, replace to a new price).
2. Decode the bytes back through `ItchFileConnector` → apply to the book.
3. Assert the reconstructed book equals the book built by applying the original events
   directly (reuse the Milestone 1 `std::map` reference), and that trade volume from
   `Trade` events matches the encoded executions.

Plus: malformed-length / truncated-message / unknown-type tests; per-message decode
latency reported in the benchmark. **Follow-up (not today):** validate against a real
NASDAQ ITCH 5.0 sample day and compare BBO/trade prints to a third-party parser.

---

## 5. Module layout after M2 + M3

```text
include/market/
  MarketDataEvent.hpp     (edit) + Reduce, Replace, new_order_id
  OrderBook.hpp           (edit) + submit(), price ladder, Reduce/Replace
  OrderTypes.hpp          (new)  OrderRequest, Fill, FillSink, enums, SubmitResult
  itch/ItchMessages.hpp   (new)  ITCH 5.0 type tags + field offsets/decoders
  itch/ItchFileConnector.hpp (new)
src/
  OrderBook.cpp           (edit) matching + ladder + Reduce/Replace
  ItchFileConnector.cpp   (new)
tests/
  MarketEngineTests.cpp   (edit) matching oracle+properties+fuzz; ITCH round-trip
benchmarks/
  Benchmark.cpp           (edit) match-latency + ITCH-decode microbench
data/
  (optional) small generated ITCH sample for a decode smoke test
```

## 6. What stays fixed (the moat)

Single-writer book on the consumer thread; SPSC transport untouched; integer ticks;
preallocated pools; `noexcept` allocation-free hot paths for `process_event` **and**
`submit`; differential oracle + property/fuzz tests; ASan/UBSan/TSan clean; benchmarks
with committed machine specs and raw output. Every M2/M3 PR must keep all of these green.

---

## 7. Transport & timing (M3 follow-ups + M4)

These layers sit *in front of* the book and are deliberately decoupled so a byte source
can be swapped without touching decode or matching.

```text
byte / datagram source            framing                decode              book
─────────────────────             ────────────           ─────────────       ──────────
InMemory / file (BinaryFILE) ─┐
LengthPrefixed file (MoldUDP) ─┼─▶ 2-byte length  ─┐
io_uring UDP socket (Linux) ──┘    MoldUDP64 hdr  ─┴─▶ itch::decode_message ─▶ process_event
```

### 7.1 Datagram source abstraction
`itch::DatagramSource` yields one datagram (one MoldUDP64 packet) at a time, each valid
until the next call. Implementations: `InMemoryDatagramSource` (tests / pre-captured),
`LengthPrefixedDatagramFileSource` (streaming file replay, 4-byte-length container), and
`IoUringDatagramSource` (Linux). This is the single seam where the transport plugs in.

### 7.2 MoldUDP64 (`itch::MoldUdp64Connector`)
Parses the 20-byte MoldUDP64 header (session, 8-byte sequence, 2-byte count) and the
length-framed message blocks, decoding each with `itch::decode_message`. Because the
header carries the sequence of the packet's first message, the connector tracks the
expected next sequence and reports **gaps** (missed sequence numbers) and skips
**overlaps** (retransmitted messages already delivered). Heartbeats (count 0) and
end-of-session (count 0xFFFF) are handled. This is the roadmap's "sequence-gap detection".

**Retransmit recovery.** An optional `RetransmitSource` models a rewind/retransmission
server: on a gap, the connector requests each missing sequence and splices recovered
messages into the delivered stream in order, so downstream sees a gap-free feed;
sequences that cannot be recovered are counted as genuinely missed. This is the recovery
*loop and interface* — a live deployment backs `RetransmitSource` with a UDP request
channel to the exchange's retransmit host.

### 7.3 Streaming reads
`ItchFileConnector` now frames from a fixed 64 KiB window refilled from disk (compact +
read), so day-sized captures need not be resident. The in-memory constructor and its
behavior are unchanged; both share one framer. `next()` stays allocation-free in steady
state (the window only grows for a message larger than itself, which ITCH never emits).

### 7.4 io_uring receive (Linux, `MARKET_ENABLE_IO_URING`)
`IoUringDatagramSource` receives UDP datagrams via io_uring (pimpl keeps liburing out of
the header). It is a `DatagramSource`, so it composes with `MoldUdp64Connector`
unchanged. **macOS cannot build or run this**; it is compiled behind a CMake flag and
covered by a loopback smoke test in the Ubuntu `io-uring` CI job. A zero-length datagram
is the end-of-stream sentinel used by tooling.

### 7.5 Hardware timing & affinity
`market::cycle_now()` (`Time.hpp`) reads the CPU cycle counter (x86 TSC / AArch64
`cntvct_el0`, else `steady_clock`) with a startup-calibrated ns-per-cycle factor, for
lower-overhead latency sampling than `steady_clock`. `market::pin_current_thread_to_core`
(`Affinity.hpp`) pins a thread on Linux (`pthread_setaffinity_np`) and reports `false`
where unsupported (macOS), so callers degrade gracefully.

### 7.6 SoupBinTCP (`itch::SoupBinTcpConnector`)
Frames NASDAQ's TCP session layer (2-byte length = 1-byte type + payload) and decodes the
ITCH payload of each Sequenced Data packet (`S`), advancing an implicit sequence; End of
Session (`Z`) ends the stream, and heartbeat/login/debug/unsequenced packets are skipped.
This is the reusable framing/decoding core; the TCP login handshake and heartbeat exchange
are out of scope. It reuses the same bounded-window streaming framer as ItchFileConnector.

### 7.7 `itch_replay` tool and real-data validation
`src/ItchReplay.cpp` reconstructs a book from a real capture — BinaryFILE by default, or
`--mold` for a length-prefixed MoldUDP64 capture — and prints message counts, gap stats,
best bid/ask, and the state checksum. It has been run against a prefix of a real NASDAQ
TotalView-ITCH 5.0 sample day (`01302019.NASDAQ_ITCH50`), decoding **4,078,307 real
messages with zero rejects** via the streaming connector and correctly flagging the
truncated tail. Per-symbol books are now available (`MultiSymbolBook`, §7.10): the decoder
carries `stock_locate` on `MarketDataEvent`, and `itch_replay --per-symbol` routes each
symbol to its own book — on the same prefix that yields 3,378 symbols and a proper
uncrossed BBO for the busiest symbol ($159.80 × 6 / $160.00 × 100), instead of the
cross-symbol extremum a single shared book produced.

### 7.8 End-to-end io_uring pipeline benchmark
`benchmarks/IoUringPipelineBench.cpp` (Linux, behind the flag) wires a paced UDP sender ->
io_uring receive -> MoldUDP64 decode (producer, pinned) -> SPSC queue -> order book
(consumer, pinned), and reports the transport->book latency decomposition (queue
residence, book processing, end-to-end) in ns via the calibrated cycle counter. Numbers
are rough and host-dependent; on ARM the virtual counter is coarse (~24 MHz, ~42 ns/tick)
and a shared VM adds jitter and can drop datagrams (the gap counter surfaces this).

### 7.10 Per-symbol books (`MultiSymbolBook`)
`MarketDataEvent` now carries `symbol` (ITCH `stock_locate`, read at offset 1 of every
message). `MultiSymbolBook` holds one `OrderBook` per symbol, created lazily on first sight
and indexed by id for O(1) routing with no hot-path allocation; it exposes per-symbol
`best_bid`/`best_ask`, aggregate `symbol_count`/`total_orders`, and an order-independent
state checksum keyed by symbol. This is the prerequisite for per-symbol microstructure
research (see [docs/research-platform.md](docs/research-platform.md)). A single symbol's
peak resting-order count is far below a whole feed's, so each per-symbol book uses a small
fixed capacity. Building *all* symbols with independent pools is memory-bound; a shared
order arena across symbols is a future optimization (the research workflow studies one
symbol at a time, so it is not on the critical path).

### 7.11 Still open
A live SoupBinTCP session (login handshake, heartbeats) and a real UDP request channel
behind `RetransmitSource`; a shared cross-symbol order arena; multishot / registered-buffer
io_uring; and huge-page / NUMA placement remain future work.
