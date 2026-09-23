#!/usr/bin/env python3
"""Phase 2 signal-validity study for the Research-to-Execution Lab.

Reads the columnar L1 feature stream emitted by `itch_features` and answers the first
question of the two (see docs/research-platform.md §1-§3): does order-book imbalance
carry predictive information about short-horizon future mid-price moves?

It reports the rank-IC-decay curve for two features -- static top-of-book imbalance and
Cont-Kukanov-Stoikov order-flow imbalance (OFI) -- with a moving-block-bootstrap CI, and
prints the honest single-symbol caveat. It computes labels here (forward returns), so the
C++ side never needs future data: leakage-proof by construction.

Usage:
    python3 analysis/signal_validity.py features.csv [--out ic_decay.png]

numpy only (matplotlib optional; the PNG is skipped if it is unavailable).
"""
import sys
import numpy as np

# Horizons in L1-update steps (the natural microstructure sampling clock of the input).
HORIZONS = [1, 2, 5, 10, 20, 50, 100, 200, 500]
BOOTSTRAP_SAMPLES = 400
BLOCK = 200  # moving-block length; must exceed the return autocorrelation scale


def average_rank(x):
    """Ranks with ties averaged (Spearman needs this), numpy-only."""
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=float)
    ranks[order] = np.arange(len(x), dtype=float)
    # average tied ranks
    x_sorted = x[order]
    i = 0
    n = len(x)
    while i < n:
        j = i + 1
        while j < n and x_sorted[j] == x_sorted[i]:
            j += 1
        if j - i > 1:
            ranks[order[i:j]] = (ranks[order[i]] + ranks[order[j - 1]]) / 2.0
        i = j
    return ranks


def rank_ic(feature, forward):
    """Spearman rank correlation between feature and forward return."""
    if len(feature) < 3:
        return float("nan")
    fr = average_rank(feature)
    gr = average_rank(forward)
    fr -= fr.mean()
    gr -= gr.mean()
    denom = np.sqrt((fr * fr).sum() * (gr * gr).sum())
    if denom == 0:
        return float("nan")
    return float((fr * gr).sum() / denom)


def block_bootstrap_ic(feature, forward, samples=BOOTSTRAP_SAMPLES, block=BLOCK):
    """Moving-block bootstrap CI for the rank IC (honest for autocorrelated samples)."""
    n = len(feature)
    if n < block * 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(12345)
    n_blocks = n // block
    ics = []
    for _ in range(samples):
        starts = rng.integers(0, n - block, size=n_blocks)
        idx = (starts[:, None] + np.arange(block)[None, :]).reshape(-1)
        ics.append(rank_ic(feature[idx], forward[idx]))
    ics = np.array([v for v in ics if not np.isnan(v)])
    if len(ics) == 0:
        return (float("nan"), float("nan"))
    return (float(np.percentile(ics, 2.5)), float(np.percentile(ics, 97.5)))


def compute_ofi(bid_px, bid_sz, ask_px, ask_sz):
    """Cont-Kukanov-Stoikov order-flow imbalance from consecutive L1 snapshots."""
    n = len(bid_px)
    ofi = np.zeros(n)
    for k in range(1, n):
        if bid_px[k] > bid_px[k - 1]:
            e_bid = bid_sz[k]
        elif bid_px[k] == bid_px[k - 1]:
            e_bid = bid_sz[k] - bid_sz[k - 1]
        else:
            e_bid = -bid_sz[k - 1]
        if ask_px[k] < ask_px[k - 1]:
            e_ask = ask_sz[k]
        elif ask_px[k] == ask_px[k - 1]:
            e_ask = ask_sz[k] - ask_sz[k - 1]
        else:
            e_ask = -ask_sz[k - 1]
        ofi[k] = e_bid - e_ask
    return ofi


def decay_table(name, feature, mid):
    print(f"\n== {name}: rank-IC decay ==")
    print(f"{'horizon':>8} {'rank_IC':>10} {'IC_95%_CI':>22} {'dir_acc':>9} {'n':>9}")
    rows = []
    for h in HORIZONS:
        if h >= len(mid):
            continue
        fwd = mid[h:] - mid[:-h]
        feat = feature[:-h]
        valid = np.isfinite(fwd) & np.isfinite(feat)
        f, g = feat[valid], fwd[valid]
        if len(f) < 3:
            continue
        ic = rank_ic(f, g)
        lo, hi = block_bootstrap_ic(f, g)
        # directional accuracy of sign(feature) vs sign(forward), ignoring zeros
        nz = g != 0
        dir_acc = float(np.mean(np.sign(f[nz]) == np.sign(g[nz]))) if nz.any() else float("nan")
        rows.append((h, ic, lo, hi))
        print(f"{h:>8} {ic:>10.4f} {('['+format(lo,'.4f')+', '+format(hi,'.4f')+']'):>22} "
              f"{dir_acc:>9.4f} {len(f):>9}")
    return rows


def main():
    if len(sys.argv) < 2:
        print("usage: signal_validity.py features.csv [--out ic_decay.png]", file=sys.stderr)
        return 2
    path = sys.argv[1]
    out_png = None
    if "--out" in sys.argv:
        out_png = sys.argv[sys.argv.index("--out") + 1]

    data = np.genfromtxt(path, delimiter=",", names=True)
    if data.size == 0:
        print("no rows", file=sys.stderr)
        return 1
    two_sided = data["two_sided"] == 1
    bid_px = data["bid_px"][two_sided].astype(float)
    ask_px = data["ask_px"][two_sided].astype(float)
    bid_sz = data["bid_sz"][two_sided].astype(float)
    ask_sz = data["ask_sz"][two_sided].astype(float)

    mid = (bid_px + ask_px) / 2.0
    total = bid_sz + ask_sz
    imbalance = np.where(total > 0, (bid_sz - ask_sz) / total, 0.0)
    ofi = compute_ofi(bid_px, bid_sz, ask_px, ask_sz)

    print(f"rows(two-sided)={len(mid)}  spread_ticks(median)={np.median(ask_px - bid_px):.0f}")

    imb_rows = decay_table("Static top-of-book imbalance", imbalance, mid)
    ofi_rows = decay_table("Order-flow imbalance (OFI)", ofi, mid)

    print("\nCAVEAT: single symbol, single day -> this is a time-series illustration, NOT")
    print("a claim of statistical significance or alpha stability. Overlapping, autocorrelated")
    print("samples mean the IC point estimates need the block-bootstrap CI shown, and real")
    print("significance/stability requires many symbols and multiple days (walk-forward).")

    if out_png:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(7, 4.2))
            for label, rows in [("static imbalance", imb_rows), ("OFI", ofi_rows)]:
                if rows:
                    hs = [r[0] for r in rows]
                    ics = [r[1] for r in rows]
                    los = [r[2] for r in rows]
                    his = [r[3] for r in rows]
                    ax.plot(hs, ics, marker="o", label=label)
                    ax.fill_between(hs, los, his, alpha=0.15)
            ax.axhline(0, color="0.5", lw=0.8)
            ax.set_xscale("log")
            ax.set_xlabel("forward horizon (L1-update steps)")
            ax.set_ylabel("rank IC vs future mid-return")
            ax.set_title("Signal-validity: rank-IC decay (single symbol, one day)")
            ax.legend()
            fig.tight_layout()
            fig.savefig(out_png, dpi=120)
            print(f"\nwrote {out_png}")
        except Exception as exc:  # matplotlib optional
            print(f"(plot skipped: {exc})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
