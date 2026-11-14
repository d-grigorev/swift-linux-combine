# Operators

The complete catalog by category. Every example is compiled and
executed in `Tests/DocumentationExamplesTests`. For the exhaustive
declaration listing see the generated `docs/API-REFERENCE.md`
(English edition); for the surprising-but-verified behaviors see
`docs/QUIRKS.md`.

## Filtering

<!-- example: operators/filter -->

`filter`/`compactMap` keep or reshape elements; `removeDuplicates`
suppresses consecutive repeats:

```swift
var kept: [Int] = []
_ = [1, 2, 2, 3, 3, 3].publisher
    .filter { $0 % 2 == 0 }
    .removeDuplicates()
    .sink { kept.append($0) }
// kept == [2]

var mapped: [String] = []
_ = [1, 4, 9].publisher
    .compactMap { $0 == 4 ? "four" : nil }
    .sink { mapped.append($0) }
// mapped == ["four"]
```

<!-- example: operators/range-slicing -->

`drop`/`prefix`/`output(in:)` slice by position or predicate:

```swift
var kept: [Int] = []
_ = [1, 2, 3, 4, 5].publisher
    .dropFirst(2)          // ignore the first two
    .prefix(2)             // take two…
    .sink { kept.append($0) }
// kept == [3, 4]

var one: [Int] = []
_ = [10, 20, 30].publisher.output(at: 1).sink { one.append($0) }
// one == [20] — output(at:) publishes a single indexed element.
```

`drop(untilOutputFrom:)` and `prefix(untilOutputFrom:)` use a second
publisher as the tripwire; `drop(while:)`/`prefix(while:)` use a
predicate.

## Transforming

<!-- example: operators/map-family -->

```swift
struct Person { let name: String; let age: Int }
let people = [Person(name: "Ann", age: 30), Person(name: "Bo", age: 4)].publisher

var names: [String] = []
_ = people.map(\.name).sink { names.append($0) }
// names == ["Ann", "Bo"]

var pairs: [(String, Int)] = []
_ = people.map(\.name, \.age).sink { pairs.append($0) }
// pairs == [("Ann", 30), ("Bo", 4)] — key-path pair sugar.

var scanned: [Int] = []
_ = [1, 2, 3].publisher.scan(0, +).sink { scanned.append($0) }
// scanned == [1, 3, 6] — scan emits after every element.
```

`tryMap`, `mapError`, `setFailureType(to:)`, `encode(encoder:)` and
`decode(type:decoder:)` cover the throwing/failure-shaping cases (see
[Error handling](#error-handling) and [Coding](#coding)).

## Collecting

<!-- example: operators/collect -->

`collect()` buffers the whole stream into one array; `collect(n)`
emits arrays of `n`; `collect(strategy)` groups by time/count (see
[SCHEDULERS.md](SCHEDULERS.md)):

```swift
var arrays: [[Int]] = []
_ = [1, 2, 3].publisher.collect().sink { arrays.append($0) }
// arrays == [[1, 2, 3]]

var chunks: [[Int]] = []
_ = [1, 2, 3, 4, 5].publisher.collect(2).sink { chunks.append($0) }
// chunks == [[1, 2], [3, 4], [5]] — partial buffer on finish.
```

## Reducing & selection

<!-- example: operators/reduce -->

Fold to a single value, computed when the stream finishes:

```swift
var sum = 0
_ = [1, 2, 3].publisher.reduce(0, +).sink { sum = $0 }
// sum == 6

var maximum = 0
_ = [7, 3, 9].publisher.max().sink { maximum = $0 }
// maximum == 9 — min()/max() on Comparable elements.
```

<!-- example: operators/selection -->

Predicates short-circuit and cancel the upstream:

```swift
var allEven = false
_ = [2, 4, 6].publisher.allSatisfy { $0 % 2 == 0 }.sink { allEven = $0 }
// allEven == true

var hasSeven = false
_ = [1, 7, 3].publisher.contains(7).sink { hasSeven = $0 }
// hasSeven == true — emits `true` and finishes on the first match.

var total = 0
_ = [1, 2, 3, 4].publisher.count().sink { total = $0 }
// total == 4
```

The `try` variants (`tryAllSatisfy`, `tryContains(where:)`,
`tryComparison`, …) fail the stream on a thrown error.

## Combining publishers

<!-- example: operators/combine-latest -->

`combineLatest` emits whenever *any* source emits, pairing the newest
values of all sources:

```swift
let a = PassthroughSubject<Int, Never>()
let b = PassthroughSubject<String, Never>()
var pairs: [String] = []

let cancellable = a.combineLatest(b) { num, str in "\(num)\(str)" }
    .sink { pairs.append($0) }

a.send(1)
b.send("x")     // both have values now → "1x"
b.send("y")     // → "1y"
a.send(2)       // → "2y"
// pairs == ["1x", "1y", "2y"]
```

<!-- example: operators/zip -->

`zip` pairs elements positionally — element *n* of the result needs
element *n* of every source:

```swift
let names = ["Ann", "Bo", "Cy"].publisher
let ages = [30, 40, 50].publisher
var people: [String] = []

_ = names.zip(ages) { $0 + " is \($1)" }.sink { people.append($0) }
// people == ["Ann is 30", "Bo is 40", "Cy is 50"]
```

<!-- example: operators/merge -->

`merge` interleaves several sources of the same type; `MergeMany`
flattens an array of publishers:

```swift
let a = PassthroughSubject<Int, Never>()
let b = PassthroughSubject<Int, Never>()
var seen: [Int] = []

_ = Publishers.Merge(a, b).sink { seen.append($0) }
a.send(1)
b.send(2)
a.send(3)
// seen == [1, 2, 3]
```

## Sequencing publishers

<!-- example: operators/append-prepend -->

`append`/`prepend` concatenate streams:

```swift
var seen: [Int] = []
_ = [1, 2].publisher.append([3, 4]).prepend(0).sink { seen.append($0) }
// seen == [0, 1, 2, 3, 4]
```

<!-- example: operators/flatmap-switch -->

`flatMap` flattens per-element publishers; `switchToLatest` keeps only
the newest inner stream; `catch` recovers:

```swift
let outer = PassthroughSubject<Int, Never>()
var seen: [String] = []

_ = outer
    .flatMap { value in ["a\(value)", "b\(value)"].publisher }
    .sink { seen.append($0) }

outer.send(1)
// seen == ["a1", "b1"]
```

`switchToLatest` is chosen explicitly:

```swift
let outer = PassthroughSubject<PassthroughSubject<Int, Never>, Never>()
var seen: [Int] = []
_ = outer.switchToLatest().sink { seen.append($0) }

let inner1 = PassthroughSubject<Int, Never>()
let inner2 = PassthroughSubject<Int, Never>()
outer.send(inner1)
inner1.send(1)      // seen == [1]
outer.send(inner2)  // inner1 is cancelled
inner1.send(9)      // ignored
inner2.send(2)      // seen == [1, 2]
```

## Error handling

<!-- example: operators/try-catch -->

```swift
struct Oops: Error {}

var seen: [Int] = []
_ = [1, 2, 3].publisher
    .tryMap { value in
        if value == 2 { throw Oops() }
        return value
    }
    .catch { _ in Just(-1) }
    .sink { seen.append($0) }
// seen == [1, -1] — the failure was replaced by the fallback.
```

<!-- example: operators/replace-error-empty -->

`replaceError(with:)` substitutes a value; `replaceEmpty(with:)`
substitutes for a value-less finish; `retry(n)` re-subscribes on
failure:

```swift
struct Oops: Error {}

var seen: [Int] = []
_ = Fail<Int, Error>(error: Oops())
    .replaceError(with: 0)
    .sink { seen.append($0) }
// seen == [0]

var got = 0
let empty = [Int]().publisher.replaceEmpty(with: -1)
_ = empty.sink { got = $0 }
// got == -1
```

## Debugging & inspection

<!-- example: operators/print-handle-events -->

`print` logs every event with a prefix; `handleEvents` runs hooks at
every stage without altering the stream:

```swift
var hookLog: [String] = []
let subject = PassthroughSubject<Int, Never>()
var seen: [Int] = []

let cancellable = subject
    .handleEvents(receiveOutput: { hookLog.append("out \\($0)") },
                  receiveCancel: { hookLog.append("cancelled") })
    .sink { seen.append($0) }

subject.send(4)
// hookLog == ["out 4"], seen == [4]

cancellable.cancel()
// hookLog == ["out 4", "cancelled"]
```

`breakpoint(receiveOutput:)` raises `SIGTRAP` when its closure returns
`true` — a debugger hook, not a stream operator.

## Connectable publishers

<!-- example: operators/share -->

`share()` (a multicast + autoconnect) lets several subscribers share
one subscription:

```swift
let source = PassthroughSubject<Int, Never>()
let shared = source.share()
var first: [Int] = []
var second: [Int] = []

_ = shared.sink { first.append($0) }
_ = shared.sink { second.append($0) }

source.send(1)
source.send(2)
// first == [1, 2], second == [1, 2] — one upstream subscription,
// two observers.
```

<!-- example: operators/make-connectable -->

`makeConnectable()` delays the flow until `connect()` — subscribe all
your observers first, then start:

```swift
let subject = PassthroughSubject<Int, Never>()
let connectable = subject.makeConnectable()
var seen: [Int] = []
_ = connectable.sink { seen.append($0) }

subject.send(1)      // not connected → dropped
let connection = connectable.connect()
subject.send(2)      // seen == [2]
connection.cancel()
```

`multicast(subject:)` generalizes this to any subject;
`autoconnect()` connects on first subscription and cancels when the
last subscriber cancels.

## Coding

<!-- example: operators/encode-decode -->

`encode(encoder:)`/`decode(type:decoder:)` turn a stream of `Codable`
values into a stream of `Data` and back:

```swift
struct Article: Codable { let title: String; let year: Int }
let article = Article(title: "A", year: 2026)

var encoded: [Data] = []
_ = [article].publisher
    .encode(encoder: JSONEncoder())          // Failure == Error
    .sink(receiveCompletion: { _ in },
          receiveValue: { encoded.append($0) })
// encoded == [non-empty JSON data for the article]

var decoded: [Article] = []
_ = encoded.publisher
    .decode(type: Article.self, decoder: JSONDecoder())
    .sink(receiveCompletion: { _ in },
          receiveValue: { decoded.append($0) })
// decoded == [Article(title: "A", year: 2026)] — round-trip intact.
```

`replaceNil(with:)` is sugar over `Map` for optional streams.

## Timing operators

`debounce`, `throttle`, `delay`, `timeout`, `measureInterval` and the
timed `collect(strategy:)` all run on a [scheduler](SCHEDULERS.md);
their deterministic testing patterns live there.

## Full list

The generated `docs/API-REFERENCE.md` (English edition) lists every
operator type and method signature; the `Publishers` group contains
the complete set of operator implementations.
