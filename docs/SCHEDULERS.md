# Schedulers

Schedulers decide *where and when* work runs. Every timing operator
and every `receive(on:)`/`subscribe(on:)` needs one. Each example here
is compiled and executed in
`Tests/DocumentationExamplesTests`.

## The protocol

A `Scheduler` provides:

- `SchedulerTimeType` — a `Strideable` point in time;
- `now`, `minimumTolerance`;
- `schedule(options:_:)` — run soon;
- `schedule(after:tolerance:options:_:)` — run once at a time;
- `schedule(after:interval:tolerance:options:_:)` — run repeatedly,
  returning a `Cancellable`.

## ImmediateScheduler

<!-- example: schedulers/immediate -->

Executes everything synchronously, in place — the tool for making
timing operators deterministic in tests:

```swift
let subject = PassthroughSubject<Int, Never>()
var seen: [Int] = []
let cancellable = subject
    .delay(for: .seconds(5), scheduler: ImmediateScheduler.shared)
    .sink { seen.append($0) }

subject.send(1)
cancellable.cancel()
// seen == [1] — no time actually passes; the delay is a no-op.
```

## DispatchQueue

The scheduler you'll reach for most:

<!-- example: schedulers/dispatchqueue -->

```swift
let subject = PassthroughSubject<Int, Never>()
var seen: [Int] = []
let queue = DispatchQueue(label: "examples.delivery")

let cancellable = subject
    .receive(on: queue)
    .sink { seen.append($0) }

subject.send(1)
queue.sync {}        // wait for the hop
cancellable.cancel()
// The sink closure runs on `queue`, not on the caller's thread.
```

`subscribe(on:)` moves the *subscription* work (e.g. an expensive
upstream start) onto the scheduler instead:

```swift
// subscribe(on:) hops upstream attachment; receive(on:) hops every
// downstream event. Use the former for work placement, the latter for
// UI/delivery safety.
```

## RunLoop / OperationQueue

`RunLoop.current` (or `.main`) and `OperationQueue.main` are
schedulers too — same shape, date-based time:

```swift
let queue = OperationQueue()
queue.maxConcurrentOperationCount = 1
// queue.schedule(after: queue.now.advanced(by: .seconds(1)),
//                tolerance: .init(0), options: nil) { … }
```

`Timer.publish(every:on:in:)` produces a connectable publisher on a
run loop — see [FOUNDATION.md](FOUNDATION.md).

## Writing a test scheduler

The borrowed test suite drives all timing operators with a
`VirtualTimeScheduler`: a `Scheduler` whose "time" only advances when
the test says so, running scheduled work in timestamp order. The
essential pattern:

```swift
final class VirtualTimeScheduler: Scheduler {
    struct SchedulerTimeType: Strideable, Comparable {
        // a logical timestamp + Stride arithmetic…
    }
    private var work: [(date: SchedulerTimeType,
                        action: () -> Void)] = []

    var now: SchedulerTimeType { /* current logical time */ }
    var minimumTolerance: SchedulerTimeType.Stride { .init(0) }

    func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        action()                      // "soon" means: right now
    }

    func schedule(after date: SchedulerTimeType,
                  tolerance: SchedulerTimeType.Stride,
                  options: SchedulerOptions?,
                  _ action: @escaping () -> Void) {
        work.append((date, action))
    }

    func schedule(after date: SchedulerTimeType,
                  interval: SchedulerTimeType.Stride,
                  tolerance: SchedulerTimeType.Stride,
                  options: SchedulerOptions?,
                  _ action: @escaping () -> Void) -> any Cancellable {
        // enqueue repeating work; return a token that removes it
        …
    }

    func advance(to time: SchedulerTimeType) {
        // move `now` forward, running everything due, in order
    }
}
```

A complete implementation lives in
`Tests/LinuxCombineTests/Support/VirtualTimeScheduler.swift` (borrowed from
the parity suite); it drives the timing tests for `debounce`,
`throttle`, `delay`, `timeout` and `measureInterval`.

## receive(on:) vs subscribe(on:) — a table

| | hops | use for |
|---|---|---|
| `receive(on:)` | every downstream event (values, completion, subscription) | delivering to the main thread |
| `subscribe(on:)` | the upstream `subscribe` call | expensive upstream setup off the caller |

<!-- example: schedulers/receive-on -->

```swift
let queue = DispatchQueue(label: "examples.hop")
let subject = PassthroughSubject<Int, Never>()
let firstThread = Thread.current

var deliveryThread: Thread?
let expectation = expectation(description: "delivered")
let cancellable = subject
    .receive(on: queue)
    .sink { _ in
        deliveryThread = Thread.current
        expectation.fulfill()
    }

subject.send(1)
wait(for: [expectation], timeout: 5)
cancellable.cancel()
// Delivery happened on `queue`, not on the test's thread.
XCTAssertNotNil(deliveryThread)
XCTAssertFalse(deliveryThread === firstThread)
```
