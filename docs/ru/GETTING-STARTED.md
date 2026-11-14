# Начало работы

Библиотека Combine — чисто Swift-реализация одноимённого фреймворка
Apple: те же протоколы, издатели, подписчики и операторы, доступные
на Linux как `import LinuxCombine`. Каждое объявление и каждое
описываемое здесь поведение проверено против официального инвентаря
API и набора тестов паритета (см. [README.md](README.md)).

Каждый пример на этой странице компилируется и выполняется в тестовом
наборе `Tests/DocumentationExamplesTests`, поэтому приведённые
результаты соответствуют реальному поведению библиотеки.

## Установка

Добавьте пакет в описание зависимостей вашего `Package.swift`:

```swift
dependencies: [
    .package(path: "../swift-combine"),
],
targets: [
    .target(name: "MyApp", dependencies: ["Combine"]),
]
```

И импортируйте библиотеку так же, как оригинальный фреймворк:

```swift
import LinuxCombine
```

## Модель в одном экране

Издатель (`publisher`) производит последовательность значений во
времени. Чтобы их получать, к нему присоединяют подписчика
(`subscriber`). Между ними находится подписка (`subscription`) —
канал управления: подписчик объёмом спроса (`demand`) запрашивает
значения, издатель передаёт не больше запрошенного. Завершается
поток по инициативе любой из сторон: издатель передаёт завершение
(`.finished` или `.failure`), подписчик вызывает `cancel()`.

```
publisher ──subscribe──▶ subscriber
publisher ◀──demand────  subscriber
publisher ──values────▶  subscriber
publisher ──completion─▶ subscriber   (или подписчик отменяет подписку)
```

## Первые конвейеры

<!-- example: getting-started/hello-sink -->

Наиболее распространённый терминальный оператор — `sink`: он
подписывается, запрашивает неограниченный спрос и передаёт вам
замыкания для завершения и значений:

```swift
var received: [Int] = []
let cancellable = [1, 2, 3].publisher
    .map { $0 * 10 }
    .sink { received.append($0) }

// received == [10, 20, 30]
// Храните `cancellable`, пока хотите получать события.
```

Подписка является значением — сохраните её (обычно в свойстве или в
`Set`), иначе при уничтожении маркера подписка будет отменена.

<!-- example: getting-started/just-and-empty -->

Не каждый издатель передаёт поток значений. `Just` публикует ровно
одно значение и завершается; `Empty` завершается немедленно, ничего
не передав; `Fail` немедленно завершается ошибкой:

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
    // Значений не будет — завершение придёт немедленно.
}
empty.cancel()
```

<!-- example: getting-started/sequence-publisher -->

Любая последовательность (`Sequence`) превращается в издатель одним
свойством `publisher` и завершается после последнего элемента:

```swift
let numbers = [10, 20, 30].publisher   // Publishers.Sequence<Int, Never>

var seen: [Int] = []
let cancellable = numbers.sink { seen.append($0) }
cancellable.cancel()
// seen == [10, 20, 30]
```

<!-- example: getting-started/subject -->

`PassthroughSubject` — издатель, который вы приводите в движение
сами; это основной инструмент тестирования:

```swift
let subject = PassthroughSubject<Int, Never>()

var received: [Int] = []
let cancellable = subject.sink { received.append($0) }

subject.send(1)
subject.send(2)
subject.send(completion: .finished)
cancellable.cancel()
// received == [1, 2]; отправка после завершения игнорируется.
```

<!-- example: getting-started/currentvalue -->

`CurrentValueSubject` устроен так же, но хранит последнее значение и
выдаёт его каждому новому подписчику:

```swift
let counter = CurrentValueSubject<Int, Never>(0)
counter.send(5)

var seen: [Int] = []
let cancellable = counter.sink { seen.append($0) }
cancellable.cancel()
// seen == [5] — сохранённое значение приходит первым.

counter.send(6)
// seen == [5, 6]
```

<!-- example: getting-started/assign -->

Оператор `assign(to:on:)` записывает каждое значение в свойство
объекта по ключевому пути — это связь между издателем и вашей
моделью данных:

```swift
final class Model { var title = "" }
let model = Model()

let subject = PassthroughSubject<String, Never>()
let cancellable = subject.assign(to: \.title, on: model)

subject.send("hello")
cancellable.cancel()
// model.title == "hello"
```

Для свойств, соответствующих протоколу `Codable`, существует
вариант `assign(to:on:)` с ключевым путём к свойству `@Published` —
он разобран в [SUBJECTS-AND-STATE.md](SUBJECTS-AND-STATE.md).

<!-- example: getting-started/future -->

`Future` представляет значение, которое придёт один раз, — обычно
результат асинхронной задачи:

```swift
let future = Future<Int, Never> { promise in
    promise(.success(42))
}

var seen: [Int] = []
let cancellable = future.sink { seen.append($0) }
cancellable.cancel()
// seen == [42]
```

`Future` выполняет работу эжено (при создании) и запоминает
результат: каждый последующий подписчик получает одно и то же
единственное значение.

## Обработка ошибок в одну строку

<!-- example: getting-started/catch -->

Семейство операторов с префиксом `try` превращает поток с ошибками
в поток типа `Never`; `catch` подключает резервный издатель:

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

## Что читать дальше

- [CONCEPTS.md](CONCEPTS.md) — спрос и бэкпрешер, отмена, стирание
  типов, потоки выполнения;
- [OPERATORS.md](OPERATORS.md) — каталог операторов с примерами;
- [SCHEDULERS.md](SCHEDULERS.md) — планировщики, `receive(on:)`,
  собственные планировщики для тестов;
- [SUBJECTS-AND-STATE.md](SUBJECTS-AND-STATE.md) — субъекты,
  `@Published`, `ObservableObject`;
- [COMPATIBILITY.md](COMPATIBILITY.md) — границы совместимости и
  список вопросов-ответов.
