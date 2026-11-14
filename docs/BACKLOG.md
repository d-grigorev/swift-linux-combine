# Backlog & reassessment (post-M7)

Coverage as of the final batch:

- **Type level: 321/321 official symbols (100%)** —
  `docs/API-INVENTORY.md` (regenerate: `python3 Tools/audit_inventory.py`).
- **Member level: 1091/1091 official member declarations verified
  against `Sources/LinuxCombine`, 0 failures, 0 triage** —
  `docs/DECL-AUDIT.md` (regenerate: `python3 Tools/verify_decls.py`,
  non-zero exit on failure).
- **1 policy exclusion**: the other-framework extension
  `storeWhileEntityActive` (QUIRKS C5).

**Scope: EVERY planned group is DONE** (batches 1–6). The only
non-implemented official API is the KVO publisher, excluded by product
decision (docs/QUIRKS.md §D).

**Batch 6 (async interop, group 6) is DONE**: `AsyncPublisher`,
`AsyncThrowingPublisher` (+ `Element`, `Iterator` with `next()`) and
the two `Publisher.values` properties — a continuation pump (`.max(1)`
per `next()`, pre-subscription `next()` calls replayed as pending
demand, completion/cancel resume all pending with `nil`, the throwing
variant delivers a failure to the first pending continuation) with
task-cancellation handlers cancelling the upstream subscription.
Borrowed: `PublisherConcurrencyTests` (30) pass first try; TSAN clean.

**Batch 5 (sugar backends, group 5) is DONE**: `Optional.Publisher`
and `Result.Publisher` (both DocC-404 — QUIRKS C2 — shape pinned by
the 41 official Just/Sequence member declarations returning them, the
tests' compat branches, and the borrowed suites) + their full operator
sugar, plus the official `Optional.publisher`/`Result.publisher`
properties; the **84 Just/Sequence sugar members** (`Just`: 38, with
the concrete overloads shadowing the generic ones exactly as upstream
does; `Publishers.Sequence`: 36 with the consult's extension-level
constraints, which DocC hides — C3), including the dual `count()`
overloads (Collection→`Result`, RandomAccess+Never→`Just`).
Borrowed: `OptionalPublisherTests` (38), `ResultPublisherTests` (36)
+ the restored (uncarved) sugar bodies in `JustTests`/`SequenceTests`.
Tooling fix: the sugar members live on DocC **article** extension
pages (`*-Publisher-Operators`, kind=`article`) which the type-page
filter silently skipped — `verify_decls.py` now walks them under their
base type (QUIRKS C10).

**Batch 4 (connectables + buffering + AssertNoFailure, phase 4) is
DONE**: `Multicast`, `Share`, `Autoconnect`, `MakeConnectable`,
`Buffer` (+ `PrefetchStrategy`/`BufferingStrategy`), `AssertNoFailure`
and their methods (`multicast(subject:)`/`multicast(_:)`, `share()`,
`autoconnect()` on `ConnectablePublisher`, `makeConnectable()`,
`buffer(size:prefetch:whenFull:)`, `assertNoFailure(_:file:line:)`) —
all official decls verified. Borrowed: `MulticastTests`,
`ShareTests`, `AutoconnectTests`, `MakeConnectableTests`,
`BufferTests` (76), `AssertNoFailureTests`.

**Batch 3 (selection family, group 2) is DONE**: `AllSatisfy`,
`TryAllSatisfy`, `Contains`, `ContainsWhere`, `TryContainsWhere`,
`Count`, `Comparison`, `TryComparison`, `Output` + methods
(`allSatisfy`, `tryAllSatisfy`, `contains`, `contains(where:)`,
`tryContains(where:)`, `count()`, `min`/`max` sugar family,
`prefix(_:)`, `output(at:)`, `output(in:)`) on a new shared
`ReduceProducer` base (`Sources/LinuxCombine/Helpers/ReduceProducer.swift`).
The official `comparison(areInIncreasingOrder:)` / `tryComparison(...)`
methods have **no DocC pages** (404, QUIRKS C8) — signatures pinned by
the official init decls + own tests (`ComparisonSugarTests`).

**Batch 2 (quick singles, old groups 7–11) is DONE**: `Print`,
`HandleEvents`, `Breakpoint`, `Retry`, `IgnoreOutput`, `ReplaceEmpty`,
`PrefixUntilOutput`, `DropUntilOutput`, `MapKeyPath`/`2`/`3`,
`Encode`/`Decode` (+ the JSON/PropertyList Foundation tests), and the
`OperationQueue` scheduler conformance. `replaceNil(with:)` turned out
to be already present (M3 Map sugar). The OperationQueue DocC pages
needed a crawler section allowlist (its Combine section is titled
"Scheduling Operations") — see `Tools/crawl_combine_api.py`.

**Batch 1 (timing operators) is DONE**: `Debounce`, `Throttle`,
`Delay`, `Timeout`, `MeasureInterval`, `TimeGroupingStrategy`,
`CollectByCount`, `CollectByTime` + their protocol methods — borrowed
tests in place (Debounce/Throttle/Delay/Timeout/MeasureInterval/
CollectByCount) and own-test-pinned where no oracle exists
(`CollectByTimeTests`).

## Remaining groups

| # | Group | Symbols | Size | Dependencies / notes |
|---|---|---|---|---|
| 1 | **Timing operators** ~~(batch 1)~~ | `Debounce`, `Throttle`, `Delay`, `Timeout`, `MeasureInterval`, `TimeGroupingStrategy`, `CollectByCount`, `CollectByTime` | ~~M~~ ✅ | **Done** — see git log; `CollectByTime`/strategy are own-test-pinned (QUIRKS §B). |
| 2 | **Selection family** ~~(batch 3)~~ | `AllSatisfy`/`TryAllSatisfy`, `Contains`/`ContainsWhere`/`TryContainsWhere`, `Count`, `Comparison`/`TryComparison`, `Output` | ~~M~~ ✅ | **Done** — borrowed tests for all five families pass; `comparison`/`tryComparison` methods are own-test-pinned (C8). |
| 3 | **Connectables** ~~(batch 4)~~ | `Multicast`, `Share`, `Autoconnect`, `MakeConnectable` | ~~M~~ ✅ | **Done** — composed over the earlier `ConnectablePublisher` protocol. |
| 4 | **Buffering** ~~(batch 4)~~ | `Buffer` + `BufferingStrategy` + `PrefetchStrategy` | ~~M~~ ✅ | **Done** — single reentrancy-safe drain loop; `byRequest`'s surprising `.unlimited` initial pull is oracle-pinned (QUIRKS A17). |
| 5 | **Sugar backends** ~~(batch 5)~~ | `Optional.Publisher`, `Result.Publisher` + Just/Sequence sugar (84 members) | ~~M~~ ✅ | **Done** — backends DocC-blind (C2), members verified via the article-page walk (C10); all four borrowed test files pass. |
| 6 | **Async interop** ~~(batch 6)~~ | `AsyncPublisher`, `AsyncThrowingPublisher` (+`Element`/`Iterator`) | ~~M~~ ✅ | **Done** — all 30 borrowed concurrency tests pass; inventory reaches 321/321. |
| 7 | **Events & debug** ~~(batch 2)~~ | `Print`, `HandleEvents`, `Breakpoint` | ~~S~~ ✅ | **Done** — borrowed tests in place. |
| 8 | **Encoding** ~~(batch 2)~~ | `Encode`, `Decode` operators | ~~S~~ ✅ | **Done** — JSON/PropertyList parity tests pass. |
| 9 | **Terminal singles** ~~(batch 2)~~ | `Retry`, `IgnoreOutput`, `ReplaceEmpty`, `PrefixUntilOutput`, `DropUntilOutput`, `CollectByCount` | ~~S~~ ✅ | **Done** (CollectByCount landed with batch 1). |
| 10 | **MapKeyPath** ~~(batch 2)~~ | `MapKeyPath`, `MapKeyPath2`, `MapKeyPath3` | ~~S~~ ✅ | **Done** — reflection expectations relaxed for Swift 6 key-path descriptions (port note in the test). |
| 11 | **OperationQueue scheduler** ~~(batch 2)~~ | `OperationQueue` conformance | ~~S~~ ✅ | **Done** — KVO-gated delay-ready operations (QUIRKS A16). |
| 12 | KVO publisher | — | excluded | Product decision (docs/QUIRKS.md §D). |
| 13 | `AssertNoFailure` ~~(batch 4)~~ | `AssertNoFailure` | ~~S~~ ✅ | **Done** — was missing from the original grouping. |

## Suggested phase order (updated)

1. ~~Timing operators~~ ✅ done (batch 1).
2. ~~Quick singles: terminal group + MapKeyPath + encode/decode +
   OperationQueue~~ ✅ done (batch 2).
3. ~~Selection family (group 2)~~ ✅ done (batch 3).
4. ~~Connectables + Buffering + `AssertNoFailure`~~ ✅ done (batch 4).
5. ~~Sugar backends (group 5)~~ ✅ done (batch 5).
6. ~~Async interop (group 6)~~ ✅ done (batch 6).

**Nothing remains.** Inventory 321/321; the only unimplemented official
API is the KVO publisher (§D product decision); the only declaration
exclusion is `storeWhileEntityActive` (C5).

Each batch = copy its borrowed tests → implement → green gates →
regenerate `API-INVENTORY.md` + `DECL-AUDIT.md` → commit (two-commit
pattern: implementation, then borrowed coverage).
