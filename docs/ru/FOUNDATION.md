# Мосты к Foundation

Наложение на Foundation расширяет поверхность Combine для системных
типов. Вся эта часть работает на Linux; примеры выполняются в
`Tests/DocumentationExamplesTests`.

## NotificationCenter

<!-- example: foundation/notificationcenter -->

Метод `publisher(for:)` превращает уведомление в поток значений
`Notification`, фильтруемый по имени и объекту:

```swift
let center = NotificationCenter.default
let name = Notification.Name("DocsExampleNotification")

var titles: [String] = []
let cancellable = center
    .publisher(for: name)
    .compactMap { $0.userInfo?["title"] as? String }
    .sink { titles.append($0) }

center.post(name: name, object: nil, userInfo: ["title": "First"])
center.post(name: name, object: nil, userInfo: ["title": "Second"])
cancellable.cancel()
// titles == ["First", "Second"]
```

## Timer

<!-- example: foundation/timer -->

Выражение `Timer.publish(every:on:in:)` создаёт издателя,
подключаемого по подключению, который публикует даты на цикле
событий; для запуска вызовите `autoconnect()` либо `connect()`:

```swift
let ticking = Timer.publish(every: 0.05, on: .main, in: .default)
    .autoconnect()

var ticks = 0
let cancellable = ticking.sink { _ in ticks += 1 }

// Дать циклу событий время как минимум для двух срабатываний,
// затем остановиться.
let deadline = Date().addingTimeInterval(0.3)
RunLoop.current.run(until: deadline)
cancellable.cancel()
// ticks >= 2
```

## URLSession

<!-- example: foundation/urlsession -->

Издатель `DataTaskPublisher` выдаёт пару `(data, response)` и
завершается ошибкой `URLError`; его удобно встраивать прямо в
конвейер `decode`:

```swift
// На Linux URLSession находится в модуле FoundationNetworking:
// #if canImport(FoundationNetworking) import FoundationNetworking #endif

let publisher = URLSession.shared.dataTaskPublisher(
    for: URL(string: "https://example.invalid/resource")!
)
// Data == Data, Failure == URLError — далее можно .map/.decode.
XCTAssertNotNil(publisher)
```

(Исполняемый пример не трогает сеть: издатель только создаётся;
тесты с реальной сетью относятся к интеграционным наборам, а не к
документации.)

## Кодировщики

`JSONEncoder`, `JSONDecoder`, `PropertyListEncoder` и
`PropertyListDecoder` реализуют протоколы кодирования Combine
(`TopLevelEncoder`/`TopLevelDecoder`) — на них основаны операторы
`encode(encoder:)` и `decode(type:decoder:)`; см.
[OPERATORS.md](OPERATORS.md).

## RunLoop и OperationQueue как планировщики

Оба типа реализуют протокол `Scheduler` с временем из дат:

```swift
// доставка на главный цикл событий
let cancellable = subject.receive(on: .main).sink { … }

// последовательная работа в очереди операций
let queue = OperationQueue()
let token = queue.schedule(after: queue.now.advanced(by: .seconds(1)),
                           tolerance: .init(0),
                           options: nil) { … }
```

Подробности и приёмы — в [SCHEDULERS.md](SCHEDULERS.md).

## Что не мостится: KVO

Привязка к значениям (KVO) — единственный кусок Apple Combine,
который эта библиотека не реализует. Решение осознанное: KVO —
свойство рантайма Objective-C без аналога на Linux и с очень
большой поверхностью. Всё остальное наложение на Foundation
присутствует и проверяется на соответствие объявлениям; подробности
в [COMPATIBILITY.md](COMPATIBILITY.md).
