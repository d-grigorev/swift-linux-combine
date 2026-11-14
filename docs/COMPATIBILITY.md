# Compatibility

This library targets **Linux first**, with source compatibility for
Apple's Combine API, imported here as `import LinuxCombine`.

## Platform matrix

| API surface | Linux | Darwin (source-compatible) |
|---|---|---|
| Core protocols, all publishers/operators, subjects | ✅ | ✅ |
| Schedulers: Immediate, DispatchQueue, RunLoop, OperationQueue | ✅ | ✅ |
| Foundation bridges: NotificationCenter, Timer, URLSession, coders | ✅ | ✅ |
| `@Published` / `ObservableObject` | ✅ | ✅ |
| Async interop (`values`) | ✅ | ✅ |
| KVO publishers | ❌ excluded | ❌ (product decision) |

Excluded-by-design is exactly one item: KVO (an ObjC-runtime facility
with no Linux counterpart). Everything DocC documents (321 types,
1091 member declarations) is implemented and declaration-verified.

## Behavioral parity

"Parity" here is demonstrated, not asserted:

- The **borrowed OpenCombine parity suite** (more than a thousand
  tests, Apache-2.0, provenance in
  `Tests/OpenCombineParityTests/README.md`) runs against *this*
  library — behaviors that differ from the reference fail the build.
- Behaviors with **no oracle anywhere** (OpenCombine lacks them too —
  e.g. `CombineLatest`, `Merge`, `CollectByTime`) are documented
  choices, pinned by our own tests and listed in `docs/QUIRKS.md` §B.
- **Surprising-but-verified** behaviors (things Apple does that look
  wrong until you check) live in QUIRKS.md §A — e.g.
  `buffer(size:prefetch:whenFull:)` with `.byRequest` requests
  `.unlimited` at subscription.

## Known deviations & gaps

| Item | Status |
|---|---|
| KVO publishers | not implemented (above) |
| `assertCrashes`-based expectations | vacuous under SwiftPM (compiled out) — crash-parity is *not* claimed |
| DocC documentation gaps | a few APIs have no DocC page (`Optional.Publisher`, `Result.Publisher`, `comparison(areInIncreasingOrder:)`, …) — signatures pinned via official call sites + parity tests; see QUIRKS §C |
| `Publisher.combineIdentifier` | publishers are not identifiable; only subscribers/subscriptions/cancellables are (matches official) |

## Toolchain notes

- Built with Swift 6.x in the Swift 5 language mode; zero warnings is
  a gate.
- `String(describing:)` output for enum errors includes module
  qualification — don't compare error descriptions in tests; pattern
  match `Subscribers.Completion` instead (the docs do).
- DocC's *rendered* declarations hide extension-level `where` clauses;
  constraints in this library were taken from the crawl + parity
  tests, not from rendered pages (QUIRKS §C).

## FAQ

**Why does `buffer(..., prefetch: .byRequest, ...)` request
`.unlimited` upstream?** Because Apple's does — pinned by the borrowed
request-history tests (QUIRKS §A17).

**Why does `output(at: -1)` crash?** The official `init(upstream:range:)`
carries non-negative preconditions; negative indices reach it via
`output(at:)` (parity-pinned).

**Can I drop this into an iOS project?** The API is source-compatible
with Apple's Combine, but the module is named `LinuxCombine` (Apple
platforms already ship the real framework under `Combine`) — this
project exists for Linux and for study.

**Where do I report a behavioral mismatch?** Anything failing in the
parity suite is a bug; anything documented in QUIRKS §B is a known,
tested choice — change it only with an updated oracle.
