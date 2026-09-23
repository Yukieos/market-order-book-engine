# Roadmap: from replay engine to a low-latency market platform

_Last updated: 2026-09-20_

This document summarizes where the project stands, scans what is currently hot in
open-source low-latency / market / trading systems, and proposes a staged plan that
moves toward those trends **without discarding the one thing this repo already does
better than most: rigor** (determinism, differential testing, clean sanitizers,
honest benchmarking).

---

## 1. Where we are today (Milestone 1)

A C++20 deterministic market-data **replay + limit-order-book** engine:

- **Pipeline:** CSV (or optional Boost.Beast TLS WebSocket) producer thread →
  bounded lock-free **SPSC ring buffer** ([SpscQueue.hpp](include/market/SpscQueue.hpp))
  → fixed-capacity **L2/L3 order book** consumer thread ([OrderBook.cpp](src/OrderBook.cpp)).
- **Hot-path discipline:** integer tick prices; preallocated order/level pools;
  open-addressed hash indexes for order-ID and price-level lookup; intrusive per-level
  FIFO lists for price-time priority; `process_event()` is `noexcept` with **0 measured
  heap allocations**.
- **Correctness:** differential test against a `std::map` reference (hand-written +
  20k seeded random events), 64-bit order-independent state checksums, an SPSC
  concurrency test, and clean **ASan / UBSan / TSan** in CI ([ci.yml](.github/workflows/ci.yml)).
- **Benchmarking:** honest latency decomposition (parse / enqueue / queue residence /
  book processing / end-to-end), a sustainable-capacity SLO search, and externally
  printed checksums so the compiler can't elide the loop. Reported ~24.3M events/s
  hot-path, ~3.95M events/s sustainable on an M2 Pro.

**Deliberate non-goals (today):** no matching of crossing orders, no strategy/risk/PnL,
no FIX, no exchange router, best-price queries scan the level array, transport is a
generic synchronous WebSocket.

**The moat.** Most public order-book repos are correctness-optional speed claims. This
one has a reference oracle, sanitizer coverage, and a benchmark methodology that resists
self-deception. Every milestone below must keep that bar — a differential oracle and a
reproducible benchmark are acceptance criteria, not afterthoughts.

---

## 2. What's hot in open source right now (2025–2026 scan)

Themes that repeatedly show up across trending GitHub projects, papers, and infra blogs:

1. **Matching engines are table stakes.** Nearly every trending "order book" repo is a
   *matching* engine: price-time priority, partial fills, market/limit/stop, IOC/FOK,
   single-threaded matching core fed by a lock-free ingress queue. Sub-microsecond and
   even sub-130ns matching latency is routinely claimed.
   (e.g. [RyanJHamby/order-book-engine](https://github.com/RyanJHamby/order-book-engine),
   [Konkyana-Samith/Limit-Order-Booking](https://github.com/Konkyana-Samith/Limit-Order-Booking))
2. **Real market-data protocols, not synthetic CSV.** NASDAQ **TotalView-ITCH 5.0** over
   **MoldUDP64 / SoupBinTCP**, and FIX. Zero-allocation binary decoders (sub-50ns per
   message), full order-book reconstruction from real day files, exported to Parquet/numpy.
   (e.g. [harris2001/UltraLowLatencyFeedHandler](https://github.com/harris2001/UltraLowLatencyFeedHandler),
   [groovg/itch-book](https://github.com/groovg/itch-book),
   [bbalouki/itchcpp](https://github.com/bbalouki/itchcpp))
3. **Kernel-bypass & modern async I/O.** DPDK / AF_XDP / RDMA at the top end; **io_uring**
   (SQPOLL + `IORING_OP_SEND_ZC` zero-copy) as the accessible modern default — sub-10µs
   round trips without special NICs. Synchronous BSD sockets are now the "before" picture.
4. **Real data replay + microstructure analytics + backtesting.** Replay real crypto/equity
   L2 (Bybit/Binance), compute spread / imbalance / microprice / impact, and backtest
   VWAP/TWAP/POV/market-making with **reproducible PnL and risk**.
   (e.g. [mansoor-mamnoon/limit-order-book](https://github.com/mansoor-mamnoon/limit-order-book))
5. **Backtest/live parity, event-driven, AI-first.** [NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
   sets the reference: nanosecond-resolution event-driven core (now Rust), identical
   strategy code in backtest and live, fast enough to train RL agents.
6. **ML on the LOB.** Deep forecasting of mid-price / transactions; standardized pipelines
   (LOBFrame, LOB transformers). The infra ask: cheaply turn a reconstructed book into
   clean feature tensors in Python. ([Deep LOB forecasting, Quant Finance 2025](https://www.tandfonline.com/doi/full/10.1080/14697688.2025.2522911))
7. **Measurement & hardware rigor.** rdtsc/HW timestamping, HdrHistogram tails, CPU pinning,
   NUMA, huge pages, cache-line awareness, O(1) best-price via bit-tricks/intrinsics,
   bare-metal/EC2 runs. Holistic benchmarking (HFTPerformance) rather than one headline number.
8. **Language texture.** C++ still owns the tightest hot path (C++20/23, moving to C++26);
   Rust is rising for the platform layer (NautilusTrader); a **Python SDK/binding** is the
   near-universal accessibility move.

---

## 3. Gap analysis: this repo vs. the trend

| Trend | This repo today | Gap |
|---|---|---|
| Matching engine | Book only; crossing orders retained | **Largest divergence** — no match/fill |
| Real protocol ingest (ITCH/FIX) | Synthetic CSV + generic WSS | No binary decoder, no real data |
| Kernel-bypass / io_uring | Synchronous Boost.Beast WSS | No io_uring/AF_XDP; not Linux-tuned |
| Microstructure analytics | best_bid/ask only | No imbalance/microprice/impact |
| Backtest + PnL/risk | None | No strategy or backtest surface |
| ML feature export / Python | C++ only | No bindings, no Parquet/tensor export |
| O(1) best price | Scans level array | No ordered index / bitset intrinsics |
| HW timing, pinning, NUMA, hugepages | steady_clock, yield() | Linux perf tuning untouched |

**Read:** the *engineering rigor* is ahead of the pack; the *feature surface* and
*realism* are behind it. The plan trades on the former to close the latter.

---

## 4. Recommended direction (north star)

> **Grow Milestone 1 into a deterministic, real-data limit-order-book platform with a
> true matching core, a zero-allocation binary feed handler, io_uring transport, and a
> thin research/backtest surface — every layer covered by a reference oracle and a
> reproducible benchmark.**

Why this framing over the alternatives:

- It **reuses the moat.** The SPSC pipeline, pooled book, differential oracle, and
  benchmark harness are exactly the substrate a matching engine + feed handler need.
- It hits the **three highest-signal trends at once** (matching, real-protocol ingest,
  modern async I/O) while the analytics/Python layer opens the ML and backtest crowd.
- It stays **honest and finishable** — each milestone is independently shippable and
  testable, not a rewrite.

### Decision forks (these change emphasis, not the spine)

- **Audience.** *Portfolio/interview* → prioritize M2 (matching) + M3 (ITCH) + a crisp
  benchmark writeup. *Research tool* → prioritize M5 (analytics/Python) + M6 (backtest).
  *Production ambitions* → M4 (io_uring, Linux perf) matters most. Recommended default:
  **portfolio-grade depth first** (M2→M3→M4), analytics/backtest as the second act.
- **OS target.** The hottest perf work (io_uring, huge pages, ITCH multicast, NUMA) is
  **Linux**. Dev machine is macOS/M2. Recommend a cheap Linux box (or EC2/bare-metal) as
  the canonical perf target and keep macOS as the portable correctness target. CI already
  runs Ubuntu — lean on it.
- **Language.** Stay C++ for the core. Add a **Python binding** (pybind11/nanobind) as the
  research surface rather than adopting Rust; revisit Rust only if a platform layer emerges.

---

## 5. Milestone plan

Each milestone lists **deliverable → why (trend) → acceptance bar**. The acceptance bar
always includes: differential oracle green, sanitizers clean, and a committed benchmark
number with method notes.

### M2 — Matching engine core _(closes the #1 gap)_ — ✅ done (2026-09-20)
_Delivered: `submit()` with Limit/Market × GTC/IOC/FOK, price-time priority via an O(1)
price ladder, allocation-free `FillSink`; verified against a reference matcher (20k
randomized ops) + property/TIF tests; ASan/UBSan/TSan clean; ~21M matches/s, 0 hot-path
allocations, submit p50/p99 ≈ 41/42 ns._

- **Deliverable:** an opt-in matching mode. On an aggressive add, match against the
  opposite side by price-time priority; emit fills; support **limit, market, IOC, FOK**;
  self-trade left as documented policy. Keep the current non-matching "book maintenance"
  mode behind a flag so replay-of-already-matched feeds still works.
- **Why:** matching is now the baseline expectation for this project category.
- **Acceptance:** extend the `std::map` oracle to a reference matcher; property tests for
  conservation of quantity and price-time fairness; fuzz add/cancel/match sequences;
  matching-path allocation count stays 0; report match latency p50/p99 via HdrHistogram.

### M3 — Real market data: NASDAQ ITCH 5.0 feed handler _(realism)_ — ✅ core + follow-ups (2026-09-20)
_Delivered: stateless zero-alloc `itch::decode_message` for the book-affecting subset
(A/F/E/C/X/D/U) and `itch::ItchFileConnector` (2-byte framing) on the existing
`DataConnector` interface; verified by a round-trip differential vs the `std::map`
reference + malformed/skip tests; ~21M msgs/s, 0 hot-path allocations._
_Follow-ups done: **MoldUDP64** framing (`MoldUdp64Connector`) with **sequence-gap
detection** + retransmit-overlap skipping + **retransmit recovery** (`RetransmitSource`
splices recovered messages in-order); **SoupBinTCP** framing (`SoupBinTcpConnector`);
**streaming** disk reads (bounded window); `DatagramSource` seam; `itch_replay` tool —
**validated on real NASDAQ data** (`01302019.NASDAQ_ITCH50` prefix: 4,078,307 messages
decoded, 0 rejects). **Still open:** live SoupBinTCP session (login/heartbeats), a real UDP
retransmit channel behind `RetransmitSource`, per-symbol books for multi-symbol feeds, and
a checked-in expectation from a full day file._

- **Deliverable:** a zero-allocation **ITCH 5.0 decoder** over MoldUDP64 (file + UDP),
  normalizing to `MarketDataEvent`, feeding the existing SPSC → book pipeline; reconstruct
  the book from a public ITCH sample and validate against known BBO/trade prints.
  Add **sequence-gap detection** and snapshot/rebuild hooks.
- **Why:** binary protocol decoding + real-data reconstruction is a top trending skill and
  gives the benchmark real message mixes instead of synthetic modifies.
- **Acceptance:** round-trip a sample day; per-message decode latency reported; malformed/
  gapped-input tests; the reconstructed L2 matches a reference parser on the sample.
- **Optional sibling (M3b):** a crypto L2 replay adapter (Binance/Bybit depth) for a second,
  license-friendly real dataset.

### M4 — Low-latency transport & Linux perf _(modern I/O + measurement)_ — 🟡 in progress (2026-09-20)
_Delivered: **io_uring** UDP `DatagramSource` behind `MARKET_ENABLE_IO_URING` (Linux;
liburing), composing with `MoldUdp64Connector`, with a loopback smoke test in the Ubuntu
`io-uring` CI job; **hardware cycle timing** (`cycle_now` + ns/cycle calibration); **CPU
affinity** (`pin_current_thread_to_core`, Linux real / graceful-false elsewhere);
**end-to-end pipeline benchmark** (io_uring → MoldUDP64 → SPSC → book, pinned threads),
run on a real Linux kernel (colima VM): 0 drops at 120k msg/s, median transport→book
latency ~83 ns. **Still open:** clean tail-latency needs bare metal + isolated cores (VM
jitter dominates p99); multishot/registered-buffer io_uring; huge pages; NUMA placement._

- **Deliverable:** an **io_uring** transport (SQPOLL, registered buffers, zero-copy send
  where available) alongside the existing socket path; **rdtsc/HW timestamping**, CPU
  pinning, and optional huge pages behind config; document NUMA placement.
- **Why:** io_uring is the accessible face of kernel-bypass; HW timing + pinning are what
  separate credible latency claims from noisy ones.
- **Acceptance:** before/after latency histograms on the same workload; pinned vs unpinned
  jitter comparison; runs captured on a fixed Linux target with CPU/flags recorded.

### Pivot (2026-09-21): Research-to-Execution Lab — see [docs/research-platform.md](docs/research-platform.md)
Targeting **SWE + Quant Developer**. One signal (order-book imbalance) taken end to end:
a leak-free, event-driven backtester on real ITCH with an **MBO queue model** (displayed +
conservative bounds), explicit `feature/decision/arrival` time model, fixed-point
deterministic ledger + checksum, waterfall + factorial-grid attribution, markout-based
adverse selection, OFI and other baselines, and a required **failure experiment** — framed
as a _systems prototype + one-day case study_, not validated alpha. Subsumes and refocuses
M5/M6 below. Full methodology, counterfactual assumptions, and MVP acceptance checklist
live in the design doc.
- **Phase 1 — per-symbol books:** ✅ done (2026-09-23). `MultiSymbolBook` (one `OrderBook`
  per `stock_locate`); `itch_replay --per-symbol` on real NASDAQ data → 3,378 symbols,
  proper uncrossed per-symbol BBO, 0 rejects; routing/isolation test + sanitizers clean.
  Fixes the cross-symbol-BBO limitation.
- **Phase 2 — signal-validity study:** ✅ done (2026-09-23). `itch_features` emits a
  leakage-proof per-event L1 feature stream; `analysis/signal_validity.py` computes
  rank-IC decay + OFI baseline with block-bootstrap CIs. Real NASDAQ result: book
  imbalance IC ≈ 0.14 at 1–2 steps, decaying to noise by ~10–20 steps. Feature-extraction
  unit test + sanitizers clean. **Next: Phase 3 (event-driven backtest loop + time model).**

### M5 — Microstructure analytics + Python surface _(research/ML reach)_
- **Deliverable:** O(1)-ish best-price (ordered index or bitset + `tzcnt`/`lzcnt`) to unlock
  cheap top-of-book; streaming **imbalance / microprice / spread / depth** features; a
  **pybind11/nanobind** binding exposing replay + feature extraction, with **Parquet/numpy**
  export of book snapshots and features.
- **Why:** feeds the ML-on-LOB and quant-research audiences; the Python SDK is the standard
  accessibility move; O(1) best-price is a common differentiator.
- **Acceptance:** features validated against a slow reference implementation; binding smoke-
  tested in CI; exported tensors reload and match the C++ book state.

### M6 — Backtest / strategy surface _(backtest–live parity)_
- **Deliverable:** a deterministic event-driven **backtest harness** on replayed/reconstructed
  data with a minimal strategy API (e.g. a market-making or imbalance signal), producing
  **reproducible PnL and basic risk** (position, inventory, drawdown). Same event types as
  live replay to preserve parity.
- **Why:** mirrors NautilusTrader's parity philosophy; reproducible PnL is the portfolio
  money-shot.
- **Acceptance:** identical inputs → identical PnL (checksummed); a documented worked example
  from raw data to PnL chart.

### Continuous — rigor & docs
Fuzzing (libFuzzer) on parsers/book, optional model-checking of the SPSC protocol, a
`BENCHMARKS.md` with machine specs and raw output, and a short design note per milestone.

---

## 6. Suggested near-term sequence

1. **First:** make the initial git commit of Milestone 1 (repo currently has **no commits**)
   so the roadmap and subsequent milestones are reviewable as PRs.
2. **M2** (matching) — highest impact, reuses everything, closes the defining gap.
3. **M3** (ITCH) — turns synthetic benchmarks into real-data credibility.
4. Then choose the second act by audience (Section 4 forks): **M4** for perf depth, or
   **M5/M6** for research/ML reach.

---

## 7. Sources

- [mansoor-mamnoon/limit-order-book](https://github.com/mansoor-mamnoon/limit-order-book) · [RyanJHamby/order-book-engine](https://github.com/RyanJHamby/order-book-engine) · [Konkyana-Samith/Limit-Order-Booking](https://github.com/Konkyana-Samith/Limit-Order-Booking)
- [harris2001/UltraLowLatencyFeedHandler](https://github.com/harris2001/UltraLowLatencyFeedHandler) · [groovg/itch-book](https://github.com/groovg/itch-book) · [bbalouki/itchcpp](https://github.com/bbalouki/itchcpp)
- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
- [Kernel bypass: DPDK, io_uring, RDMA](https://blog.lbenicio.dev/blog/kernel-bypass-networking-dpdk-io_uring-and-the-rdma-revolution/) · [C++ Design Patterns for Low-latency Applications (arXiv 2309.04259)](https://arxiv.org/abs/2309.04259)
- [Deep limit order book forecasting: a microstructural guide (Quantitative Finance, 2025)](https://www.tandfonline.com/doi/full/10.1080/14697688.2025.2522911)
- [HFTPerformance benchmarking framework](https://medium.com/@gwrx2005/hftperformance-an-open-source-framework-for-high-frequency-trading-system-benchmarking-and-803031fe7157)
