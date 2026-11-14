# Performance challenge: swift-combine vs OpenCombine

## Methodology

- **Baseline**: OpenCombine @ `1c6f02c` — the exact commit the borrowed
  parity tests come from (same semantics ground truth).
- **Build**: `swift build -c release --product LinuxCombineBench` (Swift 6.4.1),
  run pinned to cores: `taskset -c 0-7 ./.build/release/LinuxCombineBench`.
- **Measurement**: minimum of 5 iterations per scenario, **measurement
  order interleaved** (ours/baseline back-to-back per scenario) to cancel
  allocator and CPU warm-up order bias.
- **Correctness cross-check**: every shared scenario validates its output
  count across both libraries (`workload outputs match` line) — a faster
  number with a different count would be flagged as a mismatch, not a win.
- Scenarios `combineLatestBurst` and `mergeInterleave` are
  **ours-only**: OpenCombine implements neither family (docs/QUIRKS.md §B).

## Results (x86_64 Linux, min of 5, interleaved)

| scenario | ours (ms) | opencombine (ms) | ratio |
|---|---:|---:|---:|
| subjectFanOut(4×, 200k) | 79.4 | 124.3 | **0.64×** |
| mapFilterChain(100k) | 13.7 | 13.5 | 1.01× |
| sequenceMap(200k) | 45.5 | 50.6 | **0.90×** |
| zipPairs(50k) | 223.7 | 208.0 | 1.08× |
| flatMapJust(50k) | 30.6 | 31.9 | 0.96× |
| erasedSink(100k) | 14.2 | 14.3 | 1.00× |
| anyCancellableChurn(50k) | 1.9 | 2.0 | 0.95× |
| combineLatestBurst(25k ×2) | 22.7 | n/a | — |
| mergeInterleave(2×50k) | 14.1 | n/a | — |

**Verdict**: at parity or better on 7 of 7 shared scenarios except
`zipPairs` (+8%); subject fan-out is ~35% faster than the baseline.

## What the challenge found (and fixed)

1. **`AnyCancellable` was 4.24× slower on store/release churn** — root
   cause: a per-instance `NSLock` I had added during M1 "for safety".
   Apple's implementation cancels **lock-free**; removing the lock
   restored parity (0.95×) *and* matched Apple (the borrowed suite never
   required synchronization — see docs/QUIRKS.md A11). Churn = 50k
   `AnyCancellable` creations + release-driven deinit cancellation.
2. **Zip rebuilt its children array on every access** (computed property
   returning a fresh literal — heap allocation per upstream event and per
   request). Cached at construction. The benchmark itself caught the
   subtlety: children arrive *synchronously* during `init`, so the cache
   must be populated **before** subscribing or grant/state bookkeeping
   runs against an empty list (first run reported `ours=0` pairs —
   correctness cross-check doing its job).
3. **Zip's dequeue path boxed every value through `Any` + `as!` casts**
   (protocol returned `Any`). Replaced with a typed `takeValue()`; the
   hot path is now allocation- and cast-free.
4. **Zip's `request(_:)` took a redundant second lock round-trip** when
   forwarding to children; targets are now computed under the lock the
   caller already holds (recursive-lock safe, same effective ordering as
   the reference implementation).

## Known gap / follow-up

- `zipPairs` remains ~8% slower with **structurally identical**
  semantics (same O(n²) FIFO child buffering as the baseline, same lock
  classes — `NSRecursiveLock`/`NSLock` over pthread versus
  COpenCombineHelpers' direct pthread wrappers). After the three fixes
  above the remaining delta is a small constant per event; candidates are
  the lock wrapper indirection and `ZipChild` existential dispatch.
  Not chased further: a redesign for 8% would trade away behavioral
  parity for noise-class gains. Revisit only if a real workload shows it.
- Scenario granularity: single-shot workloads (10⁵–2·10⁵ elements);
  micro-allocators (e.g. per-send retention) aren't isolated individually.

## Reproduce

```bash
swift build -c release -j 3 --product LinuxCombineBench
taskset -c 0-7 ./.build/release/LinuxCombineBench
```
