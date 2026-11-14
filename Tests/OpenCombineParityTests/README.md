# OpenCombine parity tests

Borrowed test suite proving behavioral equality with a reference
implementation.

- Source: `Tests/OpenCombineTests/**` of OpenCombine @ 1c6f02c.
- Modifications (mechanical only):
  - `import OpenCombine` → `import LinuxCombine`;
  - the crash-assert helper's test filter renamed to this module
    (`OpenCombineParityTests.`);
  - `#if OPENCOMBINE_COMPATIBILITY_TEST` import dances collapsed to
    plain `import Combine`;
  - OpenCombine's `OCombine` disambiguation namespace does not exist
    here (our module is named LinuxCombine): compat branches collapsed to
    the plain official forms — `OperationQueue`, `RunLoop`, `Timer`,
    `URLSession`, `NotificationCenter` helpers and the
    `Optional`/`Result` `makePublisher` tails;
  - OpenCombine-introspection-only assertions excluded (DebugHook-based
    `_Introspection` is an OpenCombine-internal hook system Apple's
    Combine doesn't have); marked inline with `NOTE:` comments.
- Non-mechanical adaptations (also tracked in `docs/QUIRKS.md`):
  - `DispatchQueueSchedulerTests`/`RunLoopSchedulerTests`/
    `TimerPublisherTests`/`URLSessionTests`/`NotificationCenterTests`
    compatibility `#if` branches rewritten to the plain (queue/loop/center)
    forms — no `OCombine` namespace exists here (our module is
    `LinuxCombine`), so the upstream `!canImport(Combine)` branches would
    select the same plain forms;
  - `NotificationCenterTests`: the recording mock cannot override
    non-open Foundation extensions on this toolchain — replaced with a
    marker subclass, `history` assertions stripped, tracking
    expectations recomputed for real name/object filtering; the two
    `Event` extensions dropped with it;
  - `FlatMapTests.testFlatMapReceiveSubscriptionTwice` references an
    undefined `ResultPublisher` symbol (broken upstream); a local
    typealias now resolves it to the official `Result.Publisher`;
  - `URLSessionTests` mock super-forwarding handlers wrapped for
    `@Sendable` parameters;
  - `Published`/`ObservableObject`/`RunLoop`/`Timer` typealias blocks
    collapsed to single `Combine.*` definitions;
  - `OperationQueueSchedulerTests`: `TestOperationQueue` restates
    `@unchecked Sendable` and routes its block overrides through
    closure literals for the `@Sendable` corelibs signatures;
  - `MapKeyPathTests` reflection: `.contains("KeyPath")` child
    expectations relaxed to `.anything` — Swift 6 describes key paths as
    `"\Int.<computed 0x...>"` with no "KeyPath" substring (QUIRKS A15);
  - `AutoconnectTests.testAutoconnectReflection`: the expected
    description embeds the *test module* name — adjusted
    `OpenCombineTests.` → `OpenCombineParityTests.` (inline port note).
- Attribution: original file headers retained; license in
  `LICENSE-OpenCombine`.
