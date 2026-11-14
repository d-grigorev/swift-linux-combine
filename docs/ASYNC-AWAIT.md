# Async/await interop

Combine streams can be consumed with `for await … in` directly: the
`values` property wraps any publisher as an `AsyncSequence`.

## Non-throwing streams (`Failure == Never`)

<!-- example: async-await/basic-values -->

```swift
let sequence = [1, 2, 3, 4].publisher    // Publishers.Sequence<Int, Never>

var collected: [Int] = []
for await value in sequence.values {
    collected.append(value)
}
// collected == [1, 2, 3, 4]; the loop ends when the publisher finishes.
```

## Throwing streams

<!-- example: async-await/throwing-values -->

For any other failure type, `values` yields an
`AsyncThrowingPublisher` — the loop becomes `for try await` and the
publisher's error is thrown at the point of iteration:

```swift
enum TestingDocsError: Error { case sample }

let failing = Fail<Int, TestingDocsError>(error: .sample)

var received: [Int] = []
do {
    for try await value in failing.values {
        received.append(value)
    }
    XCTFail("expected the failure to throw")
} catch {
    // error == TestingDocsError.sample
}
// received == []
```

## One element at a time

<!-- example: async-await/iterator -->

The iterator requests `.max(1)` per `next()` — demand is exactly as
lazy as your loop:

```swift
let subject = CurrentValueSubject<Int, Never>(42)
var iterator = subject.values.makeAsyncIterator()

// A value sent before any next() is not delivered: the pump only
// requests .max(1) per next() call.
let value = await iterator.next()
// value == 42
```

## Cancellation

Iterating in a `Task` that gets cancelled cancels the upstream
subscription — the pump installs a cancellation handler around every
`next()`:

```swift
let subject = PassthroughSubject<Int, Never>()
var iterator = subject.values.makeAsyncIterator()

let task = Task { await iterator.next() }
task.cancel()
// The upstream subscription is cancelled; next() returns nil.
```

## Mixing paradigms

Bridges run in both directions:

- async → Combine: wrap awaited results in a `Future` or push them
  into a `PassthroughSubject` from a `Task`;
- Combine → async: the `values` loop above, or a continuation-based
  helper around any subscriber.

Prefer one paradigm per pipeline and bridge at its edge.

## Availability

`AsyncPublisher`/`AsyncThrowingPublisher` are annotated
`@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)` —
irrelevant on Linux, required for Apple-platform source
compatibility.
