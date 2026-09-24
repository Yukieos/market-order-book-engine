# Research-to-Execution Microstructure Lab — Design

_Last updated: 2026-09-21_

## 0. What this is (and is not)

This is the design for a **systems-first microstructure research harness** built on the
existing C++ order-book / feed-handler core ([README.md](../README.md),
[ARCHITECTURE.md](../ARCHITECTURE.md)). It targets **SWE + Quant Developer** signals:
event-driven correctness, a leak-free replay/backtest engine, realistic execution
modelling, determinism, and latency — with a single microstructure signal as the vehicle.

The deliverable is framed, deliberately and in all external writeups, as:

> **A complete systems prototype and a one-day microstructure case study** — not
> validated alpha research.

Upgrading to the latter (a claim about a real, stable edge) would require multi-day data,
walk-forward validation, and cross-symbol stability, which are explicitly **out of scope**
for the first version (§11). Positioning it honestly is a feature, not a hedge.

**Depth over breadth.** One signal (order-book imbalance) is taken end to end through a
rigorous pipeline, rather than many signals done shallowly. The reusable, hard-to-fake
parts (leak-free event ordering, MBO queue modelling, determinism) are the product; the
signal is the excuse to build them.

---

## 1. The two questions (never collapsed into one Sharpe)

The project answers two **independent** questions, in order:

1. **Signal validity** — does current order-book imbalance carry stable predictive
   information about short-horizon future mid-price movement?
2. **Economic realizability** — after latency, queue position, fees, and adverse
   selection, does any of that information survive into capturable PnL?

The most valuable outcome is allowed to be **negative**, e.g.:

> _Imbalance is predictive of short-horizon mid-price direction, but most of the edge
> decays before a realistically delayed passive order reaches the queue, and the fills
> that do occur are adversely selected._

A credible negative result beats a suspicious high Sharpe.

---

## 2. Time model (the spine of "no lookahead")

Every decision carries three explicit timestamps:

| Timestamp | Meaning |
|---|---|
| `feature_time` | the moment event `e_k` is **fully processed**; features use only state ≤ here |
| `decision_time` | when the strategy emits a decision (≥ `feature_time`) |
| `arrival_time` | `decision_time + latency`; when the order actually enters the sim queue |

Rules:

- **Target uses the post-event mid.** The label for event `k` is `r[k,h] = m[k+h] − m[k]`,
  where `m[k]` is the mid **after `e_k` is applied**. Pre-event and post-event mids are
  never mixed.
- **Signal study** reports predictive power measured from `feature_time`.
- **Economic study** may only capture price movement available **after `arrival_time`**.
  This directly produces the headline comparison: predictive IC that exists at
  `feature_time` but has largely decayed by `arrival_time`.
- **Not leakage, but non-executability.** Predicting a strictly future update from
  `I[k]` computed after `e_k` is legitimate; the risk is that the horizon is so short the
  signal is mechanically true yet gone before any order can act. The three timestamps make
  that distinction measurable rather than hand-waved.
- **Event-time vs wall-clock.** Signal decay is naturally an event-time curve (IC vs `h`
  events); latency is physical (wall-clock). The two are placed on a common axis: the
  decay curve is reported in **both** event-time and wall-clock, and the economic
  comparison is done in **wall-clock** (because latency is physical). Converting a
  wall-clock latency to an event count is state-dependent (event intensity varies with
  volatility), so we do not silently assume a fixed events-per-microsecond.

---

## 3. Signal-validity methodology

- **Feature:** order-book imbalance (top-of-book and multi-level variants), computed
  incrementally in the single streaming pass.
- **Target:** `r[k,h]` on post-event mid, over a grid of horizons `h` (event-time) and
  wall-clock horizons.
- **Metrics:** rank information coefficient (Spearman) between feature and future return;
  directional accuracy / AUC; monotonicity of future return across imbalance buckets;
  and the **IC-decay curve** vs horizon (the alpha half-life).
- **Statistical honesty (single-symbol, one day):**
  - A single symbol's IC is a **time-series** correlation of overlapping, autocorrelated,
    heavy-tailed samples. Significance must use **block bootstrap** or **HAC/Newey–West**,
    never a naive i.i.d. t-stat.
  - **Cross-sectional** rank IC (rank across symbols at each timestamp) is only meaningful
    with **many symbols**; it is not available in the one-symbol MVP.
  - Therefore the MVP may **plot IC decay** but must **not** claim statistical significance
    or alpha stability. Those claims wait for multi-symbol, multi-day data (§11).
  - Rank statistics (Spearman) rather than Pearson, to blunt heavy tails.

---

## 4. Economic-realizability methodology

### 4.1 Two simulation modes — never conflated

| Mode | Purpose | Claims to model real historical fills? |
|---|---|---|
| **Synthetic exchange** (M2 matching engine) | unit tests, strategy state machine, counterparty simulation, extreme scenarios, known-result differential tests | **No** |
| **Historical ITCH replay + queue model** | evaluate a hypothetical order's fill *opportunities* against real order flow | **Limited**, bounded by the §4.3 counterfactuals |

This split prevents the classic error of shoving historical orders back into a matching
engine and mistaking the result for "what the market would have done had my order been
present." The matching engine is a **test venue**, not a claim about history.

### 4.2 Queue model (MBO order-by-order)

Order-by-order ITCH is the concrete advantage over aggregated-L2 backtests: we know the
displayed **ahead-quantity** at our price when our order would arrive, and we update it
from subsequent events at that price:

- A hypothetical passive order arriving at `arrival_time` is placed **behind all displayed
  orders resting at that price at that instant**; orders added afterward queue behind it.
- **Execution (E/C)** ahead of us reduces ahead-quantity **and** represents a trade.
- **Cancel/Delete (X/D)** ahead of us advances our queue position (reduces ahead-quantity)
  but does **not** fill us.
- **Replace (U)** ahead of us re-times price/time priority for that order.
- We are filled when displayed executions at our price consume the ahead-quantity and
  reach our position.

Two bounds are reported — and, per the MBO correction, they are **not** "ignore known
cancels vs honor them" (ignoring a known cancel is an artificial floor, not a model):

- **Displayed-queue model** — strictly follows visible MBO order references (E/X/D/U update
  ahead-quantity exactly as observed).
- **Conservative queue model** — adds a **hidden-liquidity buffer** (a queue-ahead
  multiplier, or a rule that observed executable flow must exceed visible ahead-quantity
  plus a buffer before we fill).

The band between them represents **unobservable liquidity and counterfactual uncertainty**,
which is where the real doubt lives.

### 4.3 Counterfactual assumptions (stated up front, as a credibility feature)

- **Small-order / no market impact.**
- **Historical events remain unchanged** — inserting our order does not alter others'
  behavior (we cannot know the true reaction).
- Passive order appended at the queue tail **after modeled latency**.
- **Same-timestamp determinism:** ITCH nanosecond timestamps are not unique; the canonical
  order is the ITCH message/sequence order. The hypothetical order inserts at a
  deterministic point relative to real events sharing its timestamp, so results are
  reproducible (§6).
- Hidden / iceberg liquidity and iceberg refresh priority are not observable and are
  bounded by the conservative model, not asserted.
- Results are **simulated fill opportunities and their markout**, not executable live PnL.

### 4.4 Latency, fees, and fills → adverse selection

- **Latency** delays `arrival_time`; it is a first-class swept parameter.
- **Fees / rebates** are applied per fill in fixed-point.
- **Adverse selection is measured, not added.** It is not a subtractable constant; it is
  the consequence of *which* fills the queue+latency model gives us. It is measured by
  **markout** on realized fills:

  ```
  markout[Δ] = s · (m[t+Δ] − p_fill),   s = +1 for a buy fill, −1 for a sell fill
  ```

  reported at multiple horizons Δ (event-time and wall-clock), so adverse selection can be
  seen to unfold over time. Optionally, markout on fills is compared to markout over
  moments we were *not* filled, to express adverse selection as a difference.

### 4.5 Attribution: waterfall for communication, factorial grid for rigor

Because a waterfall decomposition is **path-dependent** (adding latency before queue ≠
after), it is used only as a **communication artifact**, on one fixed, interpretable order:

1. frictionless signal upper bound (cross the spread, no costs)
2. + fees
3. + decision latency
4. + passive placement & displayed queue
5. + inventory / risk constraints

The **rigorous result** is a small **factorial grid** (coarse, 2–3 levels per axis) over
`latency × queue model × fees/rebates × order size`, with **interactions reported
separately** — especially `latency × queue`, `queue × order size`, and
`latency × signal horizon`. No Shapley attribution is implemented; the grid is the truth,
the waterfall is the story.

---

## 5. Baselines (locked; no ML kitchen sink)

Deliberately **not** XGBoost/neural nets — the project's point is systems + rigor, not a
model zoo. The baseline ladder is:

1. **Static top-of-book imbalance**
2. **Multi-level book imbalance**
3. **Cont–Kukanov–Stoikov-style order-flow imbalance (OFI)** — computed incrementally in
   the same streaming pass
4. **Inventory-aware imbalance strategy**

If static book imbalance cannot beat OFI, that is itself a clean, publishable conclusion:

> _Static book state carries weaker executable information than event-level order-flow
> changes._

---

## 6. Determinism & reproducibility

Reproducibility is scoped precisely to avoid floating-point false failures:

> **Same normalized input, config, binary version, and execution mode produce
> byte-identical canonical outputs.**

Implementation discipline:

- Prices in **integer ticks**; quantities and timestamps integer; **fees/PnL in
  fixed-point** (integer minor units, overflow-guarded).
- No dependence on `unordered_map` iteration order; all outputs **stable-sorted** by a
  canonical key.
- The **checksum covers the deterministic ledger** — normalized events, fills, positions,
  and the final PnL ledger.
- **Float analytics (Sharpe, IC, AUC) are computed in Python and are NOT checksummed** —
  they are reported, not part of the reproducibility contract. This is the clean line: a
  deterministic integer ledger in C++, float analytics on top in Python.
- Run **metadata** records git SHA, compiler, config hash, and input hash.

---

## 7. Architecture

Single-pass, event-driven, streaming — reusing the existing connectors and SPSC design,
O(events), no re-reading:

```
real ITCH → per-symbol MBO books (Phase 1)
          → event-driven backtest loop (single pass; strategy sees only past state)
               ├─ decision at t  →  enters sim queue at t + latency
               ├─ MBO queue model (ahead-qty from A/E/C/X/D/U; displayed & conservative)
               ├─ fills → markout / fees / inventory / risk limits
               └─ fixed-point ledger  →  columnar records (features / fills / PnL)
          → Python: config sweep · statistics · plots · walk-forward (later)
```

- **C++ owns** replay, per-symbol book reconstruction, feature calculation, sim orders,
  the queue model, fills, risk, and the fixed-point ledger. Hot path stays pure C++.
- **Python owns** experiment configuration, analysis, and visualization only.
- **No per-event Python callback.** That path (GIL, marshalling, C++/Python state
  consistency, callback overhead) is the scope trap we avoid. v1 has **no binding at all**:
  the C++ engine takes a **JSON/CLI config** and emits **columnar records**; Python reads
  them. pybind11 is considered **only later**, and only to expose **batch replay → NumPy
  arrays**, never per-event callbacks.
- **Columnar output without a heavy dependency.** The engine emits minimal **CSV or a
  trivial binary columnar** format; Python (`pyarrow`) converts to Parquet/Arrow if wanted.
  Arrow is kept **out of the C++ core** to preserve its dependency-free property. Binding
  is not the value; **research iteration speed** is.
- **The backtester is itself a tested systems artifact**, not a script: differential tests
  (e.g. a "always cross the spread" strategy reproduces known fills under the frictionless
  model), hand-worked queue scenarios, a historical-replay determinism test, and an
  end-to-end throughput benchmark (events/s including strategy + queue model).

---

## 8. What we reuse from the existing core

| Need | Existing piece | Gap to build |
|---|---|---|
| Real feed ingest | ITCH / MoldUDP64 / SoupBinTCP + streaming, real-data validated | — |
| Book reconstruction | `OrderBook` (pools, indexes, price ladder) | **per-symbol** instantiation |
| Event ordering (no lookahead) | event-ordered streaming replay | backtest loop wrapping it |
| Synthetic venue / tests | M2 matching engine (`submit`, TIF) | keep as test-only mode |
| Determinism | integer ticks + state checksums | extend to fixed-point PnL ledger |
| Latency tooling | `cycle_now`, calibration | reuse for markout/latency axes |
| Perf discipline | benchmark harness, sanitizers, differential oracle | apply to the backtester |

Execution/replay — the hard-to-fake half — is largely done. The work is the research half
(signal study, queue/economic model, determinism ledger, columnar output, Python analysis).

---

## 9. Phased plan (each phase shippable and tested)

1. **Per-symbol books** — ✅ done (2026-09-23). `MultiSymbolBook` routes by `stock_locate`
   (now carried on `MarketDataEvent`) to one lazily-created `OrderBook` per symbol; routing
   is an O(1) indexed lookup, no hot-path allocation. `itch_replay --per-symbol` on the real
   NASDAQ prefix: 3,378 symbols, 0 rejects, and the busiest symbol shows a proper
   uncrossed BBO ($159.80 × 6 / $160.00 × 100) — fixing the cross-symbol-extremum
   limitation. Verified by a routing/isolation test vs standalone single-symbol books,
   plus a keyed order-independent state checksum; ASan/UBSan/TSan clean.
2. **Signal-validity study** — ✅ done (2026-09-23). `itch_features` emits a per-event L1
   feature stream (one row per L1 change) for one symbol; `analysis/signal_validity.py`
   computes forward mid-returns, rank-IC decay, and an OFI baseline with a moving-block
   bootstrap CI. On the real NASDAQ prefix (symbol 676, 2,271 two-sided updates): static
   book imbalance has rank IC ≈ 0.14 at 1–2 steps (CI excludes 0), decaying into noise by
   ~10–20 steps; OFI is predictive over ~2–20 steps. Leakage-proof by construction (C++
   emits only post-event state; labels are forward shifts computed in Python). Feature
   extraction unit-tested; ASan/UBSan/TSan clean. The fast decay motivates Phase 5's
   latency sensitivity. _(Honest caveat, printed by the script: single symbol/day is an
   illustration, not significance or stability.)_
3. **Backtest loop + time model** — ✅ done (2026-09-23). `Backtester` (src/Backtest.cpp)
   is a single-pass, leak-free loop with the explicit `feature/decision/arrival` time
   model: a hysteresis imbalance strategy sees only post-event L1 state, its decision
   enters the sim at `decision + latency`, and a crossing order fills at the opposite touch
   on arrival (the frictionless upper bound; MBO queue + fees/markout are Phases 4-5). The
   ledger is integer/fixed-point, flattened at session end, and covered by a deterministic
   run checksum. `itch_backtest` runs it on a real symbol. Tests: hand-worked PnL,
   latency-delays-fill, run-to-run determinism; ASan/UBSan/TSan clean.
   _First real finding (symbol 676, pre-market prefix): the frictionless-crossing strategy
   loses ~2.15M ticks over 472 trades and PnL barely moves from 0 to 10 ms latency —
   because the ~2,900-tick pre-market spread dominates, so latency is second-order here.
   That is exactly why the signal must be tested with a **passive** strategy that does not
   pay the spread (Phases 4-5), where queue position and adverse selection become the story._
4. **MBO queue model** — displayed & conservative bounds; A/E/C/X/D/U ahead-quantity
   accounting; hand-worked scenario tests.
5. **Economic model** — latency, fees/rebates, markout/adverse-selection; waterfall +
   factorial grid + interactions.
6. **Baselines** — TOB imbalance, multi-level, OFI, inventory-aware; comparison + at least
   one **failure experiment** and its applicability boundary.
7. **Python analysis layer** — sweep + statistics + plots over the columnar outputs.

Throughput benchmark and sanitizer/differential coverage run continuously, as in the core.

---

## 10. Minimum-lovable acceptance checklist (one symbol, one day)

- [ ] per-symbol MBO book passes invariants + state checksum
- [ ] strategy accesses only current-and-past state (enforced by the loop)
- [ ] `decision_time` and `arrival_time` explicitly separated
- [ ] displayed-queue model supports A/E/C/X/D/U ahead-quantity updates
- [ ] fixed-point fill/PnL ledger
- [ ] imbalance feature vs future-mid target with **no triggering-event contamination**
- [ ] IC-decay curve (event & wall-clock) with a dependent-sample warning
- [ ] latency × queue × fee sensitivity (factorial grid) + waterfall
- [ ] fill-rate, markout (multi-horizon), fees, inventory, PnL attribution
- [ ] OFI baseline (plus TOB / multi-level / inventory-aware)
- [ ] at least one failure experiment + stated applicability boundary
- [ ] synthetic hand-worked queue tests
- [ ] historical-replay determinism test (byte-identical canonical outputs)
- [ ] end-to-end throughput benchmark (events/s incl. strategy + queue model)
- [ ] JSON/CLI config + minimal CSV/binary columnar outputs
- [ ] Python does only sweep / statistics / plotting

**External framing:** _complete systems prototype and one-day microstructure case study_ —
not validated alpha research.

---

## 11. Out of scope (v1) / limitations

- **No cross-symbol or multi-day statistical claims** — single symbol/day supports an
  IC-decay *illustration*, not significance or stability. Those need many symbols and
  walk-forward / OOS across days.
- **No live trading, no market-impact model** — small-order, history-unchanged
  counterfactual only; results are fill *opportunities*, not executable live PnL.
- **No hidden/iceberg liquidity truth** — bounded by the conservative queue model.
- **No Rust, no ML models, no per-event Python binding** — deliberate scope discipline.
- **Clean tail-latency numbers need bare metal + isolated cores**; a shared VM's p99 is
  dominated by scheduling jitter.
