# Subjects and state

Subjects are the bridge between imperative code and reactive
pipelines: they are publishers you push into by hand.

## PassthroughSubject

<!-- example: subjects/passthrough -->

No retained state — subscribers only see values sent after they
attach, and demand gates delivery:

```swift
let subject = PassthroughSubject<String, Never>()

var seen: [String] = []
let cancellable = subject.sink { seen.append($0) }

subject.send("early")
subject.send(completion: .finished)
subject.send("late")        // ignored — already finished
cancellable.cancel()
// seen == ["early"]
```

Every `send` fans out to all current subscribers; after a completion
the subject is terminal forever.

## CurrentValueSubject

<!-- example: subjects/currentvalue -->

Retains the latest value and replays it to every new subscriber —
the reactive "observable variable":

```swift
let name = CurrentValueSubject<String, Never>("initial")

var seen: [String] = []
let cancellable = name.sink { seen.append($0) }
// seen == ["initial"] — the retained value first.

name.send("next")
cancellable.cancel()
// seen == ["initial", "next"]

XCTAssertEqual(name.value, "next")
name.value = "assigning value == send"
```

## Record

<!-- example: subjects/record -->

Replays a fixed buffer to every subscriber — ideal for fixtures and
cached responses:

```swift
let history = Record<Int, Never>(output: [1, 2, 3], completion: .finished)

var first: [Int] = []
_ = history.sink(
    receiveCompletion: { _ in },
    receiveValue: { first.append($0) }
)

var second: [Int] = []
_ = history.sink(
    receiveCompletion: { _ in },
    receiveValue: { second.append($0) }
)

// Each subscriber replays the whole recording:
// first == [1, 2, 3], second == [1, 2, 3]
```

## @Published

<!-- example: subjects/published -->

`@Published` wraps a property so subscribers receive the current value
on subscription and every subsequent mutation:

```swift
final class Model {
    @Published var count = 0
}
let model = Model()

var seen: [Int] = []
let cancellable = model.$count.sink { seen.append($0) }
// seen == [0] — the projected value `$count` publishes the current one.

model.count = 1
model.count = 2
cancellable.cancel()
// seen == [0, 1, 2]
```

Assigning the same value still publishes — there is no deduplication
unless you add `removeDuplicates()`.

## ObservableObject

<!-- example: subjects/observable-object -->

Combine a class's `@Published` properties into one object-change
stream with `objectWillChange`:

```swift
final class Settings: ObservableObject {
    @Published var darkMode = false
    @Published var fontSize = 12
}

let settings = Settings()
var changeCount = 0
let cancellable = settings.objectWillChange.sink { _ in changeCount += 1 }

settings.darkMode = true
settings.fontSize = 14
cancellable.cancel()
// changeCount == 2 — one notification per published mutation.
```

`objectWillChange` fires *before* the mutation lands.

## Linux notes

- `@Published`/`ObservableObject` need no KVO — they are pure
  Combine-level machinery and work identically on Linux.
- KVO (`withObservationTracking`-style ObjC bindings) is the one
  deliberately unimplemented API (see
  [COMPATIBILITY.md](COMPATIBILITY.md)).

## Patterns

- **Test drivers:** push with `send`, assert on collected arrays.
- **Two-way binding:** `assign(to:on:)` one way; a subscriber that
  writes back the other way (guard against loops with
  `removeDuplicates()`).
- **Multicasting:** `share()`/`multicast` for one shared subscription —
  see [OPERATORS.md](OPERATORS.md).
