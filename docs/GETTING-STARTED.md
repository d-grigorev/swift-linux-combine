# Getting started

This library is a pure-Swift reimplementation of Apple's Combine
framework: the same protocols, publishers, subscribers, and operators,
usable on Linux as `import LinuxCombine`. Every declaration and
behavior in this guide is verified against the official API inventory
and the parity test suite (see [the repository README](../README.md)).

Every code example on this page is compiled and executed as part of the
test suite (`Tests/DocumentationExamplesTests`), so what you read here
is what the library actually does.

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(path: "../swift-combine"),
],
targets: [
    .target(name: "MyApp", dependencies: ["Combine"]),
]
```

and import it like the real thing:

```swift
import LinuxCombine
```

## The mental model in one screen

A *publisher* produces a sequence of values over time. To receive them
you attach a *subscriber*. Between them sits a *subscription*: the
subscriber asks for values by issuing *demand* (`.max(n)` or
`.unlimited`), the publisher delivers at most that many. Either side
can end the stream: the publisher with a *completion* (`.finished` or
`.failure`), the subscriber with `cancel()`.

```
publisher ──subscribe──▶ subscriber
publisher ◀──demand────  subscriber
publisher ──values────▶  subscriber
publisher ──completion─▶ subscriber   (or subscriber cancels)
```

## Your first pipelines

<!-- example: getting-started/hello-sink -->

The most common terminal operator is `sink`. It subscribes, requests
unlimited demand, and hands you closures for completion and values:

```swift
var received: [Int] = []
let cancellable = [1, 2, 3].publisher
    .map { $0 * 10 }
    .sink { received.append($0) }

// received == [10, 20, 30]
// Keep `cancellable` alive until you no longer want events.
```

The subscription is a value — store it (typically in a property or a
`Set`) or the stream is cancelled immediately by `deinit`.

<!-- example: getting-started/just-and-empty -->

Not every publisher streams. `Just` publishes exactly one value and
finishes; `Empty` finishes immediately without any value; `Fail`
terminates immediately with an error:

```swift
var values: [String] = []
var completions: [Subscribers.Completion<Never>] = []

let just = Just("hello").sink(
    receiveCompletion: { completions.append($0) },
    receiveValue: { values.append($0) }
)
just.cancel()
// values == ["hello"], completions == [.finished]

let empty = Empty<Int, Never>().sink { _ in
    // No values arrive — the completion comes immediately.
}
empty.cancel()
```

<!-- example: getting-started/sequence-publisher -->

Any `Sequence` becomes a publisher via the `publisher` property, and
finishes after the last element:

```swift
let numbers = [10, 20, 30].publisher   // Publishers.Sequence<Int, Never>

var seen: [Int] = []
let cancellable = numbers.sink { seen.append($0) }
cancellable.cancel()
// seen == [10, 20, 30]
```

<!-- example: getting-started/subject -->

A `PassthroughSubject` is a publisher you drive by hand — the hub of
most test code:

```swift
let subject = PassthroughSubject<Int, Never>()

var received: [Int] = []
let cancellable = subject.sink { received.append($0) }

subject.send(1)
subject.send(2)
subject.send(completion: .finished)
cancellable.cancel()
// received == [1, 2]; later sends are ignored after completion.
```

<!-- example: getting-started/currentvalue -->

`CurrentValueSubject` is the same, but retains the latest value and
replays it to every new subscriber:

```swift
let counter = CurrentValueSubject<Int, Never>(0)
counter.send(5)

var seen: [Int] = []
let cancellable = counter.sink { seen.append($0) }
cancellable.cancel()
// seen == [5] — the retained value arrives first.

counter.send(6)
// seen == [5, 6]
```

<!-- example: getting-started/assign -->

`assign(to:on:)` writes each value into a key path of an object — the
glue between a publisher and your model layer:

```swift
final class Model { var title = "" }
let model = Model()

let subject = PassthroughSubject<String, Never>()
let cancellable = subject.assign(to: \.title, on: model)

subject.send("hello")
cancellable.cancel()
// model.title == "hello"
```

For `Codable` properties there is `assign(to:on:)` with a
`Published` key path — covered in
[SUBJECTS-AND-STATE.md](SUBJECTS-AND-STATE.md).

<!-- example: getting-started/future -->

`Future` represents a value that will arrive once — typically the
result of an async task:

```swift
let future = Future<Int, Never> { promise in
    promise(.success(42))
}

var seen: [Int] = []
let cancellable = future.sink { seen.append($0) }
cancellable.cancel()
// seen == [42]
```

Futures are eager (the work starts at creation) and memoized (every
subscriber gets the same single result).

## Error handling in one line

<!-- example: getting-started/catch -->

The `try*` family turns a failing stream into a `Never`-failing one;
`catch` swaps a failed publisher for a fallback:

```swift
struct NotFound: Error {}

let failing = Fail<Int, Error>(error: NotFound())

var seen: [Int] = []
let cancellable = failing
    .catch { _ in Just(-1) }
    .sink { seen.append($0) }
cancellable.cancel()
// seen == [-1]
```

## Where to go next

- [CONCEPTS.md](CONCEPTS.md) — demand/backpressure, cancellation,
  type erasure, threading.
- [OPERATORS.md](OPERATORS.md) — the operator catalog with examples.
- [SCHEDULERS.md](SCHEDULERS.md) — timing, `receive(on:)`, custom
  schedulers.
- [SUBJECTS-AND-STATE.md](SUBJECTS-AND-STATE.md) — subjects,
  `@Published`, `ObservableObject`.
- [COMPATIBILITY.md](COMPATIBILITY.md) — the compatibility boundary
  of this implementation.
