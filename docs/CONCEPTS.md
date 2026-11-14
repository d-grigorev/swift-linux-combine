# Concepts

This page covers the machinery underneath every pipeline: demand,
cancellation, completion, identifiers, erasure, and the threading
model. Each example is compiled and executed in
`Tests/DocumentationExamplesTests`.

## Demand (backpressure)

Publishers never push more than the subscriber asked for. Demand is
`Subscribers.Demand`: `.none`, `.max(n)`, or `.unlimited`. It is
*additive* and *cumulative*.

<!-- example: concepts/demand-gating -->

A subject honors demand: with no demand outstanding, values are
dropped. `sink` requests `.unlimited`, so you rarely notice — but a
custom subscriber does:

```swift
let subject = PassthroughSubject<Int, Never>()

let subscriber = ManualDemandSubscriber<Int, Never>()
subject.subscribe(subscriber)

subject.send(1)                 // no demand yet → dropped
subscriber.request(.max(2))     // ask for two
subject.send(2)
subject.send(3)
subject.send(4)                 // demand exhausted → dropped
// subscriber.values == [2, 3]
```

Demand arithmetic works the way you'd expect:

```swift
let demand: Subscribers.Demand = .max(3) + .max(5)   // .max(8)
let unlimited: Subscribers.Demand = .unlimited + .max(5)  // .unlimited
let remainder: Subscribers.Demand = .max(5) - 3      // .max(2)
```

Some operators *transform* demand. For example `collect()` with a
count of 3 turns each downstream `.max(n)` into `.max(3 * n)` upstream,
because each array it emitted consumed three elements. Reduce-like
operators (`allSatisfy`, `count`, `min`/`max`…) request `.unlimited`
upstream on the first request and hold their single result until a
request arrives.

## Completions and cancellation

<!-- example: concepts/completion -->

A stream ends exactly once, with either case of
`Subscribers.Completion`:

```swift
enum ExampleError: Error { case boom }

var completion: Subscribers.Completion<ExampleError>?
var received: [Int] = []
_ = Fail<Int, ExampleError>(error: .boom)
    .sink(
        receiveCompletion: { completion = $0 },
        receiveValue: { received.append($0) }
    )
// received == []; completion == .failure(.boom) — pattern-match
// completions rather than comparing descriptions (they carry module
// qualification, which is environment-specific).
```

After a completion — or after `cancel()` — the subscription is inert:
no further values, no second completion.

<!-- example: concepts/anycancellable -->

`AnyCancellable` is the RAII token of Combine: cancelling on
deinit unless you call `cancel()` yourself, and storable in the usual
collections:

```swift
var bag = Set<AnyCancellable>()
let subject = PassthroughSubject<Int, Never>()

var received: [Int] = []
subject.sink { received.append($0) }.store(in: &bag)

subject.send(1)
bag.removeAll()                 // cancels everything
subject.send(2)
// received == [1]
```

## CombineIdentifier

Every subscriber and subscription carries a stable identity
(`combineIdentifier`), used by operators like `combineLatest` to tell
upstreams apart and by tests to pin identity. Conform your own types
via `CustomCombineIdentifierConvertible`.

## Type erasure

<!-- example: concepts/erasure -->

Concrete pipeline types get unwieldy fast; erasure trades a small heap
allocation for an opaque name:

```swift
let source: AnyPublisher<Int, Never> = [1, 2, 3].publisher.eraseToAnyPublisher()
let token: AnyCancellable = Just(1).sink { _ in }
```

`AnyPublisher` simply boxes the pipeline or token.

## Threading model

The library is thread-safe under its documented contract:

- **Subjects and multi-subscriber operators** (`PassthroughSubject`,
  `Publishers.Merge`/`CombineLatest`/`Zip`, subjects' fan-out) are
  internally locked; concurrent `send`/`subscribe` are safe.
- **Single-shot value publishers** (`Just`, `Optional.Publisher`,
  `Result.Publisher`) are deliberately *lock-free*: their inner holds
  the downstream in a plain optional that is cleared on first delivery.
  Under the contract (one logical producer) that is race-free; the
  borrowed lifecycle tests pin this design.
- **Which scheduler delivers on which thread** is always explicit:
  `receive(on:)` hops delivery through a scheduler; nothing hops
  implicitly. See [SCHEDULERS.md](SCHEDULERS.md).

<!-- example: concepts/thread-safe-subject -->

Concurrent sends to a subject from multiple threads are safe; the
per-subscriber delivery order is whatever the lock serializes:

```swift
let subject = PassthroughSubject<Int, Never>()
var received: [Int] = []
let cancellable = subject.sink { received.append($0) }

DispatchQueue.concurrentPerform(iterations: 100) { i in
    subject.send(i)
}
// received.count == 100 — every value delivered exactly once.
```

## Single-pass, multi-subscriber semantics

A publisher may be subscribed repeatedly — each subscription is an
independent stream — with two reference-semantics exceptions:
`Future` (one shared eager result) and `Share` (one shared
subscription; see [OPERATORS.md](OPERATORS.md)).

## Demand + threading: one trap worth knowing

Operators that call user closures never hold their internal locks
while doing so — your closures may safely re-enter (request demand,
cancel, even subscribe again). This is pinned by the parity suite and
is the reason `handleEvents` hooks can call back into the pipeline
safely.
