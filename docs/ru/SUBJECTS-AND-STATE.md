# Субъекты и состояние

Субъекты (`subject`) — это мост между императивным кодом и
реактивными конвейерами: издатели, которым вы сами отправляете
значения.

## PassthroughSubject

<!-- example: subjects/passthrough -->

Не хранит состояния: подписчики видят только те значения, которые
отправлены после их подписки, а спрос определяет, будет ли значение
доставлено:

```swift
let subject = PassthroughSubject<String, Never>()

var seen: [String] = []
let cancellable = subject.sink { seen.append($0) }

subject.send("early")
subject.send(completion: .finished)
subject.send("late")        // игнорируется — поток уже завершён
cancellable.cancel()
// seen == ["early"]
```

Каждый `send` распределяется по всем текущим подписчикам; после
завершения субъект навсегда остаётся терминальным.

## CurrentValueSubject

<!-- example: subjects/currentvalue -->

Хранит последнее значение и выдаёт его каждому новому подписчику —
реактивный «наблюдаемый переменный»:

```swift
let name = CurrentValueSubject<String, Never>("initial")

var seen: [String] = []
let cancellable = name.sink { seen.append($0) }
// seen == ["initial"] — сначала сохранённое значение.

name.send("next")
cancellable.cancel()
// seen == ["initial", "next"]

XCTAssertEqual(name.value, "next")
name.value = "assigning value == send"
```

## Record

<!-- example: subjects/record -->

Воспроизводит фиксированный буфер каждому подписчику — удобно для
фикстур и закешированных ответов:

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

// Каждый подписчик получает всю запись:
// first == [1, 2, 3], second == [1, 2, 3]
```

## @Published

<!-- example: subjects/published -->

Директива `@Published` оборачивает свойство так, что подписчики
получают текущее значение при подписке и каждое последующее
изменение:

```swift
final class Model {
    @Published var count = 0
}
let model = Model()

var seen: [Int] = []
let cancellable = model.$count.sink { seen.append($0) }
// seen == [0] — проекция `$count` публикует текущее значение.

model.count = 1
model.count = 2
cancellable.cancel()
// seen == [0, 1, 2]
```

Присвоение того же самого значения также публикуется: дедупликации
нет, если вы не добавили `removeDuplicates()`.

## ObservableObject

<!-- example: subjects/observable-object -->

Класс с несколькими свойствами `@Published` сводит их в один поток
изменений объекта через `objectWillChange`:

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
// changeCount == 2 — одно уведомление на каждое изменение свойства.
```

Уведомление `objectWillChange` приходит *до* того, как изменение
фактически происходит.

## Замечания для Linux

- `@Published` и `ObservableObject` не опираются на KVO: это
  чистая механика уровня Combine, работающая на Linux одинаково.
- KVO (привязка через Objective-C рантайм) — единственная
  сознательно нереализованная часть исходного API; см.
  [COMPATIBILITY.md](COMPATIBILITY.md).

## Практические приёмы

- **Управление тестами:** отправляйте значения через `send`,
  проверяйте накопленные массивы.
- **Двусторонняя привязка:** `assign(to:on:)` в одну сторону, а
  подписчик, записывающий обратно, — в другую (защититесь от петли
  с помощью `removeDuplicates()`).
- **Разветвление на несколько подписчиков:** `share()` и `multicast`
  для общей подписки — см. [OPERATORS.md](OPERATORS.md).
