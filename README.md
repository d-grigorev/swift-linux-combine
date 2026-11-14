# LinuxCombine — a pure-Swift Combine reimplementation for Linux

A from-scratch implementation of Apple's Combine framework in Swift,
specified from Apple's official documentation (DocC crawl) and
validated by a borrowed OpenCombine test suite used as a parity
oracle. Works on Linux as `import LinuxCombine` — an API-compatible
reimplementation of Apple's Combine.

**Coverage: 321/321 official type-level symbols (100%); member-level
verification: 1091/1091 declarations, 0 failures** — regenerate any
time:

```bash
python3 Tools/audit_inventory.py      # type-level coverage
python3 Tools/verify_decls.py         # member-level verification
```

## Status

| Milestone | Content | Status |
|---|---|---|
| M0 | package scaffold, DocC crawler, API inventory | ✅ |
| M1 | core protocols, `Subscribers.Demand`, type erasure, `CombineIdentifier` | ✅ |
| M2 | subjects + convenience publishers (`Just`, `Empty`, `Fail`, `Deferred`, `Future`, `Record`, `Sequence`) | ✅ |
| — | parity suite `Tests/OpenCombineParityTests` + parity-driven fixes | ✅ |
| M3 | `Sink`/`Assign` + 26 operator types | ✅ |
| M4 | `CombineLatest`/`Zip`/`Merge` families, `FlatMap`, `SwitchToLatest`, `Concatenate`, `Catch`, `ReplaceError` | ✅ |
| M5 | `Scheduler`, `ImmediateScheduler`, `DispatchQueue`, `receive(on:)`, `subscribe(on:)` | ✅ |
| M6 | `@Published`, `ObservableObject`, `NotificationCenter`, `Timer`, `RunLoop`, `URLSession` | ✅ |
| M7 | audit: coverage checklist, member-level declaration verifier, README, deviation register, `docs/BACKLOG.md` | ✅ |
| B1 | timing operators (`Debounce`, `Throttle`, `Delay`, `Timeout`, `MeasureInterval`, `TimeGroupingStrategy`, `CollectByCount`, `CollectByTime`) | ✅ |
| B2 | quick singles (`Print`, `HandleEvents`, `Breakpoint`, `Retry`, `IgnoreOutput`, `ReplaceEmpty`, `PrefixUntilOutput`, `DropUntilOutput`, `MapKeyPath` family, `Encode`/`Decode`, `OperationQueue` scheduler) | ✅ |
| B3 | selection family (`AllSatisfy`/`TryAllSatisfy`, `Contains`/`ContainsWhere`/`TryContainsWhere`, `Count`, `Comparison`/`TryComparison`, `Output` + `prefix`/`output` sugar) | ✅ |
| B4 | connectables + buffering (`Multicast`, `Share`, `Autoconnect`, `MakeConnectable`, `Buffer` + strategies, `AssertNoFailure`) | ✅ |
| B5 | sugar backends (`Optional.Publisher`, `Result.Publisher` + properties, 84 Just/Sequence sugar members) | ✅ |
| B6 | async interop (`AsyncPublisher`, `AsyncThrowingPublisher`, `Publisher.values` ×2) — **scope complete** | ✅ |

Gates (all green):

```bash
swift test -j 3                      # three suites, 0 warnings
swift test --sanitize=thread -j 3    # TSAN: 0 races
python3 Tools/verify_decls.py        # member declarations
python3 Tools/audit_inventory.py     # type coverage
python3 Tools/check_doc_examples.py  # docs ↔ executed examples
```

Performance challenge vs OpenCombine: `docs/PERF.md`
(`swift build -c release -j 3 --product LinuxCombineBench && taskset -c 0-7
./.build/release/LinuxCombineBench`).

## How the specification is sourced

- `Tools/crawl_combine_api.py` walks Apple's DocC JSON into
  `docs/api-inventory.json` (every official declaration, member page,
  and section). **This is the only step that touches the network**: the
  first run downloads ~1150 pages (≈200 s) into
  `Tools/.apple-docc-cache/`, a local cache that is git-ignored; every
  later run is served from it. All build and gate steps — `swift
  build`, `swift test`, `verify_decls`, `audit_inventory`,
  `gen_api_reference --check`, `check_doc_examples` — are fully offline
  and consume the committed `docs/api-inventory.json`; no SwiftPM
  target references the cache;
- `Tools/verify_decls.py` checks every official member declaration
  against `Sources/LinuxCombine` and writes `docs/DECL-AUDIT.md`
  (non-zero exit on drift);
- OpenCombine (pinned `1c6f02c`) is consulted only to resolve
  ambiguity — never copied;
- behaviors with no oracle anywhere are documented choices pinned by
  own tests (`docs/QUIRKS.md` §B).

## Test suites

| Suite | Purpose |
|---|---|
| `Tests/OpenCombineParityTests` | borrowed OpenCombine tests (Apache-2.0) proving behavioral equality; provenance + adaptations in its README |
| `Tests/LinuxCombineTests` | own tests for the no-oracle operators (`CollectByTime`, `CombineLatest`, `Merge`, `comparison` sugar) |
| `Tests/DocumentationExamplesTests` | every `<!-- example: … -->` snippet in `docs/*.md` compiled and executed (52 markers, EN+RU) |

## Layout

```
Sources/LinuxCombine/
  Core/          protocols, Demand, Completion, identifiers, subscriptions
  Erasure/       AnyPublisher / AnySubscriber / AnyCancellable / relay
  Publishers/    subjects, convenience publishers, operators, receive(on:)…
  Subscribers/   Sink, Assign
  Schedulers/    Scheduler, ImmediateScheduler
  Dispatch/      DispatchQueue conformance
  Foundation/    Published, ObservableObject, RunLoop/Timer/NotificationCenter/URLSession/OperationQueue
  Helpers/       Lock, ConduitBase, SubscriptionStatus, ReduceProducer
```

## Documentation

User-facing guides in `docs/` (every code example in them is compiled
and executed by the `DocumentationExamplesTests` target; the mapping
is enforced by `Tools/check_doc_examples.py`):

| Guide | Contents |
|---|---|
| [GETTING-STARTED.md](docs/GETTING-STARTED.md) | mental model, first pipelines, subjects, `Future`, error handling |
| [CONCEPTS.md](docs/CONCEPTS.md) | demand/backpressure, cancellation, erasure, threading model |
| [OPERATORS.md](docs/OPERATORS.md) | the operator catalog by category, with examples |
| [SCHEDULERS.md](docs/SCHEDULERS.md) | provided schedulers, `receive(on:)` vs `subscribe(on:)`, custom test schedulers |
| [SUBJECTS-AND-STATE.md](docs/SUBJECTS-AND-STATE.md) | subjects, `@Published`, `ObservableObject` |
| [FOUNDATION.md](docs/FOUNDATION.md) | NotificationCenter, Timer, URLSession, coders; the KVO gap |
| [ASYNC-AWAIT.md](docs/ASYNC-AWAIT.md) | `values`, cancellation, mixing paradigms |
| [COMPATIBILITY.md](docs/COMPATIBILITY.md) | platform matrix, parity methodology, FAQ |
| [API-REFERENCE.md](docs/API-REFERENCE.md) | generated full declaration listing (321 types) |

Engineering docs: [QUIRKS.md](docs/QUIRKS.md) (behavior register),
[PERF.md](docs/PERF.md), [DECL-AUDIT.md](docs/DECL-AUDIT.md),
[API-INVENTORY.md](docs/API-INVENTORY.md), [BACKLOG.md](docs/BACKLOG.md).

The whole set renders to a single PDF
(`docs/LinuxCombine-Documentation.pdf`) via
`python3 Tools/render_docs_pdf.py` (deps:
`Tools/requirements-pdf.txt`).

A **Russian edition** for readers new to Apple frameworks lives in
`docs/ru/` (introductory chapter + nine translated guides; the same
example markers, so the same executed tests cover both locales) and
renders with `python3 Tools/render_docs_pdf.py --lang ru` to
`docs/ru/LinuxCombine-Documentation-RU.pdf`.

## Scope

Complete: 321/321 official symbols implemented and declaration-verified.
The single remaining excluded item is the KVO publisher (product
decision — [COMPATIBILITY.md](docs/COMPATIBILITY.md),
[QUIRKS.md](docs/QUIRKS.md) §D).

## Licensing

The project's own work — `Sources/`, `Tools/`, `Tests/LinuxCombineTests`,
`Tests/DocumentationExamplesTests`, the documentation in `docs/`, and
this README — is licensed under the **MIT License** (see
[`LICENSE`](LICENSE), Copyright (c) 2026 Denis Grigorev).

One component is **not** covered by the MIT license:

| Component | License | Notes |
|---|---|---|
| `Tests/OpenCombineParityTests/**` | Apache-2.0 | Borrowed from [OpenCombine](https://github.com/OpenCombine/OpenCombine) @ `1c6f02c`, adapted to this module; original file headers and [`LICENSE-OpenCombine`](Tests/OpenCombineParityTests/LICENSE-OpenCombine) are retained, as the license requires. The exception is also stated in `LICENSE` itself. |

Specification source: the API surface is derived from Apple's
published DocC documentation for Combine; the crawled declarations are
factual API signatures. The implementation contains no verbatim
OpenCombine code — `docs/QUIRKS.md` is the provenance register that
records how each semantic ambiguity was resolved (official docs first,
the reference consulted to disambiguate, never copied), and
`Tools/crawl_combine_api.py` reproduces the specification crawl.

Build dependencies (the OpenCombine package used by the parity suite
and the benchmark) remain under their own licenses and are pinned in
`Package.swift`.

## Credits

- **[MiMo-V2.6-Flash](https://github.com/XiaomiMiMo)** — the AI model
  that designed and implemented this library and wrote its test suites,
  tooling, and both documentation editions (English and Russian),
  operating through the OpenCode coding agent. The model authored the
  parity-suite adaptations, the verification toolchain, the performance
  comparison harness, and this README.
- **[OpenCombine](https://github.com/OpenCombine/OpenCombine)** — the
  reference implementation consulted to resolve semantic ambiguities
  (never copied), and the source of the Apache-2.0 parity test suite
  that demonstrates behavioral equality.
- **Apple** — the Combine API and its official DocC documentation, used
  as the specification source.
- **The Swift open-source community** — the toolchain this project is
  built and tested with.
