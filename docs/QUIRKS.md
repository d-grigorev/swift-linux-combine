# Quirk & deviation register

Every entry records *what*, *why we believe it* (source), and *status*.
Sources follow the project policy: **Apple official docs first; OpenCombine
consultable to resolve ambiguity, never copied**. Status vocabulary:

- **pinned-by-oracle** — asserted by borrowed OpenCombine tests (validated
  against real Apple Combine): behavior matches Apple by construction.
- **own-test-pinned** — no oracle exists anywhere; pinned by our own tests
  (first candidates to re-verify if an Apple oracle ever appears).
- **tooling** — build/test-infrastructure observation, not a behavior claim.
- **deferred / drift-candidate** — tracked work item for M7 audit.

---

## A. Apple behaviors we now implement (surprising, but oracle-pinned)

| # | Quirk | Source |
|---|---|---|
| A1 | `Drop`: downstream subscription granted **eagerly** at `receive(subscriber:)` (before any upstream subscription); requests are buffered and flushed as `pending + remainingSkipCount` (skip pre-compensation); values and completions are never status-gated; a completion **detaches the upstream without cancelling it**; `cancel()` after completion is a silent no-op; a second upstream subscription is attached only if none is held, otherwise cancelled. | `DropTests` (`testLateSubscription`, `testReusableSubscription`, `testDroppingTwoElements`, `testCancelAlreadyCancelled`) |
| A2 | Reduce family (`Reduce`/`Collect`/`First*`/`Last*`): pull `.unlimited` from upstream immediately; **grant the downstream subscription *before* requesting** (the grant callback may observe empty upstream history); downstream demand is **never** forwarded upstream; the result pends until downstream demand even if upstream finished earlier; failures are delivered immediately; later completions/inputs are dropped. | `ReduceTests`/`CollectTests`/`FirstTests`/`LastTests` `testRequestsUnlimitedThenSendsSubscription`, `testUpstreamFinishesImmediately*`, `testBasicReductionBehavior` |
| A3 | `TryScan`'s value path is **fully blind** — the transform runs and the result is forwarded even after completion, cancellation, or a prior throw — while only the **first** throw fails the stream (later throws swallowed). Plain `Scan` is blind in every path including completions. | `ScanTests.testTryScanSuccess`, `testTryScanCancel`, `testTryScanFailureBecauseOfThrow`, `testScanCancel` |
| A4 | Try-filter family (`TryFilter`/`TryCompactMap`): values are dropped **only once terminal** (awaiting/subscribed still execute the predicate); **cancel-before-upstream-subscription marks terminal**, so a late-arriving subscription is rejected with `cancel()` (expected `[.cancelled]`) — while in the reduce family the same cancel is a **no-op** (expected `[.requested(.unlimited)]`). Two official behaviors coexist per family. | `CommonTests.testCancelBeforeSubscription` variants; `FilterTests`/`CompactMapTests`/`PrefixWhileTests`/`RemoveDuplicatesTests` cancelBefore tests |
| A5 | `Future`: **eager** (attempt closure runs in `init`); first promise wins; success is demand-gated, failure immediate; the conduit's **parent link is retained on request-time delivery but cleared on promise-time delivery** (asymmetric; `assertParent` inspects it during and after callbacks). | `FutureTests.testFutureSuccess` vs `testWaitsForDemandSuccess`, `FutureTests` (borrowed) |
| A6 | `Subscribers.Sink`/`Assign`: closures / the assigned `object` are released **after unlocking** (deadlock class documented upstream: releasing can run arbitrary user code that re-enters). `Assign` holds a **strong** reference to the object until `receive(completion:)`, then nils it — not weak. | `SinkTests`, `AssignTests`, official `Assign` docs |
| A7 | `AnyCancellable` cancels on deinit and at most once; `AnyPublisher.description` is the fixed string `"AnyPublisher"` (not the wrapped publisher's); double erasure shares one box (bit-pattern verified by tests). | `AnyCancellableTests`, `AnyPublisherTests.testDoubleErasure` |
| A8 | `Subscribers.Completion` encodes as `{"success": bool}` / `{"success": false, "error": …}` (not Swift's synthesized enum format). `CombineIdentifier` is a single `UInt64 rawValue` rendered `"0x…"`. | `CompletionTests`, `CombineIdentifierTests` (Apple-validated expectations) |
| A9 | Demand subtraction **clamps** negative results to `.none` per the official doc comment ("any operation that would result in a negative value is clamped"). OpenCombine's *code* would trap there instead (its own doc comment contradicts its implementation) — **docs won**. | Official DocC abstract; consult OpenCombine `Subscribers.Demand.swift` |
| A10 | `Zip` delivers a value downstream even when demand is zero (only the bookkeeping subtracts), and demand arriving *during* a downstream delivery is returned from the triggering child's `receive(_:)` instead of a later `request(_:)`. Zip children must hold the inner `unowned` or the downstream subscriber leaks. | `ZipTests` incl. lifecycle tests; consult OpenCombine `Publishers.Zip.swift` |
| A11 | `AnyCancellable.cancel()`/deinit are **unsynchronized** (Apple parity) — a per-instance lock measured 4.24× slower on store/release churn in the performance challenge; removed in favor of matching Apple (see docs/PERF.md). Concurrent `cancel()` from multiple threads is racy exactly as it is on Apple's implementation. | `AnyCancellableTests` (behavior), docs/PERF.md (perf evidence) |
| A12 | `collect(_ count:)` forwards each downstream request as `.max(demand * count)` upstream (an array of `count` elements = one downstream unit) and multiplies the downstream's returned demand back by `count` on emission. | `CollectByCountTests` |
| A13 | `measureInterval(using:options:)` accepts `options` in its official signature but ignores them (mirrored). | official signature + consult |
| A14 | `Debounce`'s due-timer entry is taken regardless of whether it is `.pending` or `.active` — required by the immediate scheduler, whose timers fire synchronously during scheduling. | `DebounceTests.testDebounceWithImmediateScheduler` |
| A15 | The `MapKeyPath` reflection test relaxes the borrowed `.contains("KeyPath")` child expectation to `.anything`: Swift 6 describes key paths as `\Int.<computed 0x…>` with no "KeyPath" substring (toolchain description change; mirror child names/values are still asserted). | port note in `MapKeyPathTests` |
| A16 | The `OperationQueue` scheduler realizes delays with KVO-gated (`willChangeValue(for: \…isReady)`) `DelayReadyOperation`s added to the queue *immediately* — required by the borrowed scheduler tests (history records the add; readiness gates execution; repeating chains enqueue the next operation from `main()`). | `OperationQueueSchedulerTests` |
| A17 | `buffer(size:prefetch:whenFull:)` with `.byRequest` requests **`.unlimited`** from the upstream at subscription — contradicting the official doc prose ("performs requests on demand") but exactly what the borrowed request-history tests expect. `.keepFull` requests `.max(size)` (and +1 per delivered element). Upstream demand is established *before* the downstream is granted the subscription. | `BufferTests` request histories |

## B. Our documented choices — **no oracle exists anywhere** (own-test-pinned)

OpenCombine implements **neither** `CombineLatest` **nor** `Merge`, and the
borrowed suite contains no tests for them. The following choices come from
the official contract plus internal consistency, and are pinned only by our
own suites (`CombineLatestTests`, `MergeTests`) — **first candidates for
re-verification against a real Apple oracle (e.g. a macOS cross-check):**

- All sides are pulled `.unlimited` once every side's subscription has
  arrived (grant-after-all-arrived).
- Emissions are gated by outstanding downstream demand; a trigger arriving
  with no demand is **dropped** — the latest-value state (`CombineLatest`)
  or interleave state (`Merge`) re-emits on the next event (no buffering).
- `CombineLatest` finishes only when **both/all sides finish** (a finished
  side keeps its latest value for pairing); the first failure cancels
  siblings and terminates immediately.
- `Merge` finishes when **all** upstreams finish; first failure terminates
  and cancels the siblings' upstream subscriptions.
- `CollectByTime` / `TimeGroupingStrategy` (batch 1): OpenCombine implements
  neither and the borrowed suite has no oracle — own-test-pinned by
  `LinuxCombineTests/CollectByTimeTests`: unlimited upstream pull; a window with
  no elements emits nothing; groups are demand-held until requested;
  completion flushes the partial group; failures drop it.

## C. Tooling / source quirks

| # | Quirk | Status |
|---|---|---|
| C1 | `assertCrashes`-based tests are **vacuous** under SwiftPM builds: the helper's `#if !Xcode` body is compiled out (`-D Xcode` is present in SwiftPM's flags). Crash-semantics tests (`dropFirst(-1)`, `.crash` expectations) pass trivially — **not evidence of crash parity**. | tooling |
| C2 | DocC gaps: `Optional.Publisher`, `Result.Publisher` and `Sequence.publisher` are **404** in the DocC JSON although the API exists. (Resolved for the backends in batch 5: implemented from official sugar-member decls + test compat branches + consult; pinned by `OptionalPublisherTests`/`ResultPublisherTests`.) Sources: call-site declarations in official `Just`/`Sequence` member decls + OpenCombine parity. | tooling |
| C3 | DocC omits **extension-level `where` clauses** in rendered member declarations — caused early misreadings of `sink(receiveValue:)`, `assign(to:on:)`, `removeDuplicates()`, `Drop`-family constraints. Resolve via type-soundness reasoning + borrowed tests. | tooling |
| C4 | OpenCombine upstream defects found during the work: `FlatMapTests.testFlatMapReceiveSubscriptionTwice` references an undefined `ResultPublisher` symbol (substituted `Just`, inline `NOTE` in the test); OpenCombine lacks `CombineLatest`/`Merge` entirely; its Demand `-` code contradicts its own lifted Apple doc comment. | tooling |
| C5 | The `Cancellable` DocC page lists `storeWhileEntityActive(_:)` — an extension from another framework, not Combine; excluded. | tooling |
| C6 | Crawl mechanics: member inventories come from `topicSections` identifiers; pages without any inbound DocC link (e.g. `Optional/Publisher`) never enter the crawl. | tooling |
| C7 | `OperationQueue`'s DocC page titles its Combine conformance section "Scheduling Operations" instead of the usual "Scheduling Combine Publisher…" wording, so the crawler's combine-titled-section rule skipped its 7 members entirely. Fixed with `EXTERNAL_SECTION_ALLOWLIST` in `Tools/crawl_combine_api.py` (applied both to enqueueing and link extraction); crawl grew to 1149 pages. | tooling |
| C8 | The official `comparison(areInIncreasingOrder:)` and `tryComparison(areInIncreasingOrder:)` methods have **no DocC pages at all** in the current documentation JSON (both hashed and unhashed URLs 404) — a C2-class gap. Implemented with signatures mirroring the verified official `init(upstream:areInIncreasingOrder:)` (label/semantics identical) and pinned by own tests (`LinuxCombineTests/ComparisonSugarTests`); the borrowed `ComparisonTests` cover the `min`/`max` sugar instead. | tooling |
| C9 | Adding `min()`/`max()` to `Publisher` **shadows the global `Swift.min`/`Swift.max`** inside every `extension Publisher`/`extension Publishers.*` in the module — free-function calls like `max(0, count)` started resolving to the instance method (compile errors). All such calls are now written `Swift.max(...)`/`Swift.min(...)`; keep this in mind when adding operators that introduce new name collisions. | Swift name lookup; batch 3 build |
| C10 | The bulk of the Just/Sequence sugar members are listed only on **DocC article extension pages** (`Just-Publisher-Operators`, `Publishers-Sequence-Publisher-Operators`, `kind: "article"`), not on the type pages — while `verify_decls.py` filtered on type pages (`kind: "symbol"`), silently skipping 83 members (they appeared in neither `checked` nor `excluded`). Fixed: the walker now attributes those article sections to their base type. Watch for this class: article-titled member hubs are invisible to every kind-based filter. | batch 5 verify audit |

## D. Explicit deviations & deferred items (M7 audit inputs)

- **Drift candidate**: my `Zip.request(_:)` uses a debug-only `assert` for
  `.none`, while the consulted reference uses a release-visible
  `fatalError`. Unresolved — align to `fatalError` or document.
- **Drift candidate**: `Concatenate: Equatable` (official member list) not
  yet implemented.
- Deferred by scope: `assign(to: inout Published.Publisher)` (M6, needs
  `Published`); Just/Sequence operator-sugar families and their
  `Optional.Publisher`/`Result.Publisher` backends (backlog); async `values`
  interop (`AsyncPublisher`/`AsyncThrowingPublisher`, backlog); RunLoop /
  OperationQueue as general schedulers (M5/M6 — RunLoop lands with
  `Timer.publish`); KVO (excluded by decision).
- Chosen semantics with weak doc coverage to re-audit at M7: `Future`
  async `value` continuation behavior; `Record.Recording.encode(into:)`
  format (implemented as a forward to `encode(to:)` with the official
  keyed `output`/`completion` container).

### M5 additions

- **Dispatch `@Sendable` wrappers**: the official `schedule(options:_:)`
  signature keeps a plain `() -> Void`, but swift-corelibs-libdispatch
  annotates `async`/`asyncAfter`'s `execute` as `@Sendable` — the witnesses
  wrap the action in a local `@Sendable` closure to keep the official
  signature warning-free (not a semantic change).
- **`DispatchQueue.SchedulerTimeType.Stride.*` returns 0**: stride
  multiplication (and `*=`) produces zero, mirroring the consulted
  OpenCombine reference (`DispatchTimeInterval` has no public multiply).
  Kept as-is pending contrary evidence from `DispatchQueueSchedulerTests`
  (which passed) — flagged because a no-op `*` is surprising.
- **Stride Codable wire format**: encodes under the key `_nanoseconds`
  (property name) — pinned by `testStrideCodable`; `__guessFromUnknown`
  conversion init made public for the parity suite's conversion tests.
- **`OCombine` namespace absent**: OpenCombine's `DispatchQueue.OCombine` /
  `.ocombine` disambiguation exists only because Apple's Combine occupies
  the module name on Darwin; on this platform the queue itself conforms to
  `Scheduler` directly (official shape). The borrowed test's
  compatibility `#if` was rewritten accordingly (inline NOTE).
- `SubscriptionStatus` gained `pendingTerminal` (used by `receive(on:)`'s
  scheduled completion) — same state machine as the consulted reference.

### M6 additions

- **`Timer.publish` ignores `tolerance`** — an Apple bug preserved
  faithfully (the parameter is accepted and dropped; pinned by
  `TimerPublisherTests`).
- **NotificationCenter test-double portability**: on this toolchain
  `post`/`addObserver`/`removeObserver` live in non-open Foundation
  extensions (FoundationEssentials), so OpenCombine's recording mock —
  which overrides them and asserts a `history` log — cannot exist. The
  mock became a marker subclass; history assertions were stripped and the
  four tracking expectations were **recomputed for real-center
  semantics**: name+object filtering, and corelibs drops
  `post(name:object: nil)` for observers registered with a specific
  object (Darwin Foundation documents nil as "any object" — platform
  difference, noted here).
- **`NotificationCenter.Publisher: Equatable`** — official after all
  (the borrowed `testEquatable` requires it; center identity + name +
  object identity).
- **`@Sendable` wrappers** in three places: `RunLoop.schedule`'s
  `perform`, `TimerPublisher`'s fire block, `URLSession`'s task handler
  (official signatures stay plain `() -> Void`), plus the borrowed
  URLSession test-double's nine super-forwarding handlers.
- **`assign(to: inout Published.Publisher)`** holds the subject weakly:
  when the `Published` storage dies, upstream values become no-ops rather
  than an explicit cancel (consulted implementation; pinned by
  `PublishedTests`).
- **`Published`** publishes in the setter path (`willSet` ordering per
  official docs — subscribers see the new value before the property
  changes), storage in a reference box with the `_enclosingInstance`
  subscript; `objectWillChange` installs one shared lazily-created
  publisher across all `@Published` fields via `Mirror`.

## E. Provenance footer

Borrowed tests: OpenCombine @ `1c6f02c`, Apache-2.0 — see
`Tests/OpenCombineParityTests/README.md` for the full adaptation list.
Consults (behavior reference only): OpenCombine sources for Zip, FlatMap,
SwitchToLatest, Concatenate, Catch, ReplaceError, Sink, Assign, Future,
subjects, Demand, Subscriptions — semantics studied, code written fresh.
