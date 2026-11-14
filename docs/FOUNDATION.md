# Foundation bridges

The Foundation overlay adds Combine surface to system types. All of
it works on Linux; examples execute in
`Tests/DocumentationExamplesTests`.

## NotificationCenter

<!-- example: foundation/notificationcenter -->

`publisher(for:)` turns a notification into a stream of
`Notification` values, filterable by name and object:

```swift
let center = NotificationCenter.default
let name = Notification.Name("DocsExampleNotification")

var titles: [String] = []
let cancellable = center
    .publisher(for: name)
    .compactMap { $0.userInfo?["title"] as? String }
    .sink { titles.append($0) }

center.post(name: name, object: nil,
            userInfo: ["title": "First"])
center.post(name: name, object: nil,
            userInfo: ["title": "Second"])
cancellable.cancel()
// titles == ["First", "Second"]
```

## Timer

<!-- example: foundation/timer -->

`Timer.publish(every:on:in:)` yields a *connectable* publisher of
dates on a run loop — call `autoconnect()` (or `connect()`) to start:

```swift
let ticking = Timer.publish(every: 0.05, on: .main, in: .default)
    .autoconnect()

var ticks = 0
let cancellable = ticking.sink { _ in ticks += 1 }

// Give the run loop time for at least two ticks, then stop.
let deadline = Date().addingTimeInterval(0.3)
RunLoop.current.run(until: deadline)
cancellable.cancel()
// ticks >= 2
```

## URLSession

<!-- example: foundation/urlsession -->

`DataTaskPublisher` emits `(data, response)` and fails with
`URLError`; wire it straight into the `decode` pipeline:

```swift
// On Linux, URLSession lives in the FoundationNetworking module:
// #if canImport(FoundationNetworking) import FoundationNetworking #endif

let publisher = URLSession.shared.dataTaskPublisher(
    for: URL(string: "https://example.invalid/resource")!
)
// Data == Data, Failure == URLError — feed .map/.decode downstream.
XCTAssertNotNil(publisher)
```

(No network is touched in the executed example — the publisher is
only constructed; real-network tests need connectivity and belong in
integration suites, not documentation tests.)

## Coders

`JSONEncoder`, `JSONDecoder`, `PropertyListEncoder` and
`PropertyListDecoder` all conform to the Combine coding protocols
(`TopLevelEncoder`/`TopLevelDecoder`), which is what powers
`encode(encoder:)`/`decode(type:decoder:)` — see
[OPERATORS.md](OPERATORS.md#coding).

## RunLoop / OperationQueue as schedulers

Both conform to `Scheduler` with date-based time:

```swift
// delivery on the main run loop
let cancellable = subject.receive(on: .main).sink { … }

// serial work on an operation queue
let queue = OperationQueue()
let token = queue.schedule(after: queue.now.advanced(by: .seconds(1)),
                           tolerance: .init(0),
                           options: nil) { … }
```

Details and patterns in [SCHEDULERS.md](SCHEDULERS.md).

## What is NOT bridged: KVO

Key-Value Observing is the single piece of Apple's Combine that this
library does not implement — a deliberate product decision, since
KVO is an Objective-C runtime facility with no Linux counterpart and
a large surface of its own. Everything else in the Foundation overlay
is present and declaration-verified. See
[COMPATIBILITY.md](COMPATIBILITY.md).
