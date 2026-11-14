# Планировщики

Планировщик (`scheduler`) определяет, *где и когда* выполняется
работа. Он необходим каждому оператору времени и каждому переходу
`receive(on:)` либо `subscribe(on:)`. Все примеры этой страницы
компилируются и выполняются в `Tests/DocumentationExamplesTests`.

## Протокол

Планировщик предоставляет:

- `SchedulerTimeType` — момент времени, реализующий `Strideable`;
- `now` и `minimumTolerance`;
- `schedule(options:_:)` — выполнить в ближайшее время;
- `schedule(after:tolerance:options:_:)` — выполнить однократно в
  указанный момент;
- `schedule(after:interval:tolerance:options:_:)` — выполнять
  повторно, возвращая объект `Cancellable`.

## ImmediateScheduler

<!-- example: schedulers/immediate -->

Выполняет работу синхронно на месте — главный инструмент
детерминированного тестирования операторов времени:

```swift
let subject = PassthroughSubject<Int, Never>()
var seen: [Int] = []
let cancellable = subject
    .delay(for: .seconds(5), scheduler: ImmediateScheduler.shared)
    .sink { seen.append($0) }

subject.send(1)
cancellable.cancel()
// seen == [1] — время не проходит, задержка ничего не меняет.
```

## DispatchQueue

Самый используемый планировщик:

<!-- example: schedulers/dispatchqueue -->

```swift
let subject = PassthroughSubject<Int, Never>()
var seen: [Int] = []
let queue = DispatchQueue(label: "examples.delivery")

let cancellable = subject
    .receive(on: queue)
    .sink { seen.append($0) }

subject.send(1)
queue.sync {}        // дождаться перехода
cancellable.cancel()
// seen == [1] — замыкание sink выполняется на `queue`.
```

Оператор `subscribe(on:)` переносит на планировщик *работу по
подключению* (например, дорогую настройку источника) иным образом:

```swift
// subscribe(on:) переносит подключение к источнику сверху;
// receive(on:) переносит каждое событие внизу. Первый нужен для
// размещения работы, второй — для безопасной доставки в главный
// поток.
```

## RunLoop и OperationQueue

`RunLoop.current` (или `.main`) и `OperationQueue.main` также
являются планировщиками: та же форма, время измеряется датами:

```swift
let queue = OperationQueue()
queue.maxConcurrentOperationCount = 1
// queue.schedule(after: queue.now.advanced(by: .seconds(1)),
//                tolerance: .init(0), options: nil) { … }
```

Выражение `Timer.publish(every:on:in:)` создаёт издателя,
подключаемого по подключению, на цикле событий — см.
[FOUNDATION.md](FOUNDATION.md).

## Собственный планировщик для тестов

Набор тестов паритета управляет всеми операторами времени с помощью
`VirtualTimeScheduler`: планировщика, чьё «время» продвигается только
по команде теста, а запланированная работа выполняется в порядке
временных меток. Основной паттерн таков:

```swift
final class VirtualTimeScheduler: Scheduler {
    struct SchedulerTimeType: Strideable, Comparable {
        // логическая метка времени + арифметика шага…
    }
    private var work: [(date: SchedulerTimeType,
                        action: () -> Void)] = []

    var now: SchedulerTimeType { /* текущее логическое время */ }
    var minimumTolerance: SchedulerTimeType.Stride { .init(0) }

    func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        action()                      // «в ближайшее время» = немедленно
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
        // поставить повторяющуюся работу в очередь; вернуть маркер,
        // удаляющий её
        …
    }

    func advance(to time: SchedulerTimeType) {
        // продвинуть `now`, выполнив всю просроченную работу по порядку
    }
}
```

Полная реализация находится в
`Tests/LinuxCombineTests/Support/VirtualTimeScheduler.swift` (заимствована
из набора тестов паритета); именно ею управляются тесты операторов
`debounce`, `throttle`, `delay`, `timeout` и `measureInterval`.

## receive(on:) и subscribe(on:) — сводка

| Оператор | Что переносит | Типичное применение |
|---|---|---|
| `receive(on:)` | каждое событие вниз по потоку (значения, завершение, подписку) | доставка в главный поток |
| `subscribe(on:)` | вызов `subscribe` источника | дорогая настройка источника вне вызывающего потока |

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
// Доставка произошла на `queue`, а не в потоке теста.
```
