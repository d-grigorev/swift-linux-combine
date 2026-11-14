# Операторы

Полный каталог по категориям. Каждый пример компилируется и
выполняется в `Tests/DocumentationExamplesTests`. Полный список
объявлений приведён в `docs/API-REFERENCE.md` (английское издание);
поведенческие особенности, закреплённые тестами, перечислены в
`docs/QUIRKS.md`.

## Фильтрация

<!-- example: operators/filter -->

Операторы `filter` и `compactMap` отбирают или преобразуют элементы,
а `removeDuplicates` подавляет одинаковые соседние значения:

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

Семейства `drop`, `prefix` и `output(in:)` режут поток по позиции
либо по предикату:

```swift
var kept: [Int] = []
_ = [1, 2, 3, 4, 5].publisher
    .dropFirst(2)          // игнорировать первые два элемента
    .prefix(2)             // взять два…
    .sink { kept.append($0) }
// kept == [3, 4]

var one: [Int] = []
_ = [10, 20, 30].publisher.output(at: 1).sink { one.append($0) }
// one == [20] — output(at:) публикует один элемент по индексу.
```

Операторы `drop(untilOutputFrom:)` и `prefix(untilOutputFrom:)`
используют второй издатель как триггер, а `drop(while:)` и
`prefix(while:)` — предикат.

## Преобразование

<!-- example: operators/map-family -->

```swift
struct Person { let name: String; let age: Int }
let people = [Person(name: "Ann", age: 30), Person(name: "Bo", age: 4)].publisher

var names: [String] = []
_ = people.map(\.name).sink { names.append($0) }
// names == ["Ann", "Bo"]

var pairs: [(String, Int)] = []
_ = people.map(\.name, \.age).sink { pairs.append($0) }
// pairs == [("Ann", 30), ("Bo", 4)] — пара ключевых путей.

var scanned: [Int] = []
_ = [1, 2, 3].publisher.scan(0, +).sink { scanned.append($0) }
// scanned == [1, 3, 6] — scan публикует промежуточные суммы.
```

Операторы `tryMap`, `mapError`, `setFailureType(to:)`,
`encode(encoder:)` и `decode(type:decoder:)` покрывают бросающие
преобразования и изменение типа ошибки (см. разделы «Обработка
ошибок» и «Кодирование»).

## Сборка в массивы

<!-- example: operators/collect -->

Оператор `collect()` накапливает весь поток в один массив,
`collect(n)` выдаёт массивы по `n` элементов, а `collect(strategy:)`
группирует по времени и количеству (см.
[SCHEDULERS.md](SCHEDULERS.md)):

```swift
var arrays: [[Int]] = []
_ = [1, 2, 3].publisher.collect().sink { arrays.append($0) }
// arrays == [[1, 2, 3]]

var chunks: [[Int]] = []
_ = [1, 2, 3, 4, 5].publisher.collect(2).sink { chunks.append($0) }
// chunks == [[1, 2], [3, 4], [5]] — неполный буфер допубликуется
// при завершении.
```

## Сведение и выборки

<!-- example: operators/reduce -->

Сведение к одному значению выполняется, когда поток завершается:

```swift
var sum = 0
_ = [1, 2, 3].publisher.reduce(0, +).sink { sum = $0 }
// sum == 6

var maximum = 0
_ = [7, 3, 9].publisher.max().sink { maximum = $0 }
// maximum == 9 — min()/max() для элементов, сравнимых между собой.
```

<!-- example: operators/selection -->

Предикатные операторы прерывают поток досрочно, отменяя издателя:

```swift
var allEven = false
_ = [2, 4, 6].publisher.allSatisfy { $0 % 2 == 0 }.sink { allEven = $0 }
// allEven == true

var hasSeven = false
_ = [1, 7, 3].publisher.contains(7).sink { hasSeven = $0 }
// hasSeven == true — публикуется `true` и поток завершается на
// первом совпадении.

var total = 0
_ = [1, 2, 3, 4].publisher.count().sink { total = $0 }
// total == 4
```

Варианты с префиксом `try` (`tryAllSatisfy`, `tryContains(where:)`,
`tryComparison` и другие) завершают поток ошибкой, если замыкание
бросает исключение.

## Объединение издателей

<!-- example: operators/combine-latest -->

Оператор `combineLatest` публикует значение всякий раз, когда
публикует *любой* из источников, сочетая свежие значения всех
потоков:

```swift
let a = PassthroughSubject<Int, Never>()
let b = PassthroughSubject<String, Never>()
var pairs: [String] = []

let cancellable = a.combineLatest(b) { num, str in "\(num)\(str)" }
    .sink { pairs.append($0) }

a.send(1)
b.send("x")     // оба потока имеют значения -> "1x"
b.send("y")     // -> "1y"
a.send(2)       // -> "2y"
// pairs == ["1x", "1y", "2y"]
```

<!-- example: operators/zip -->

Оператор `zip` соединяет элементы по позициям: n-й элемент результата
требует n-го элемента каждого источника:

```swift
let names = ["Ann", "Bo", "Cy"].publisher
let ages = [30, 40, 50].publisher
var people: [String] = []

_ = names.zip(ages) { $0 + " is \($1)" }.sink { people.append($0) }
// people == ["Ann is 30", "Bo is 40", "Cy is 50"]
```

<!-- example: operators/merge -->

Оператор `merge` перемешивает несколько однотипных источников, а
`MergeMany` сглаживает массив издателей в один поток:

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

## Последовательное соединение

<!-- example: operators/append-prepend -->

Операторы `append` и `prepend` присоединяют потоки последовательно:

```swift
var seen: [Int] = []
_ = [1, 2].publisher.append([3, 4]).prepend(0).sink { seen.append($0) }
// seen == [0, 1, 2, 3, 4]
```

<!-- example: operators/flatmap-switch -->

Оператор `flatMap` сглаживает издатели, порождаемые для каждого
элемента; `switchToLatest` оставляет только самый свежий внутренний
поток; `catch` восстанавливает работу после ошибки:

```swift
let outer = PassthroughSubject<Int, Never>()
var seen: [String] = []

_ = outer
    .flatMap { value in ["a\(value)", "b\(value)"].publisher }
    .sink { seen.append($0) }

outer.send(1)
// seen == ["a1", "b1"]
```

`switchToLatest` выбирается явным образом:

```swift
let outer = PassthroughSubject<PassthroughSubject<Int, Never>, Never>()
var seen: [Int] = []
_ = outer.switchToLatest().sink { seen.append($0) }

let inner1 = PassthroughSubject<Int, Never>()
let inner2 = PassthroughSubject<Int, Never>()
outer.send(inner1)
inner1.send(1)      // seen == [1]
outer.send(inner2)  // inner1 отменён
inner1.send(9)      // игнорируется
inner2.send(2)      // seen == [1, 2]
```

## Обработка ошибок

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
// seen == [1, -1] — ошибка заменена резервным значением.
```

<!-- example: operators/replace-error-empty -->

Оператор `replaceError(with:)` подставляет значение вместо ошибки,
`replaceEmpty(with:)` — вместо завершения без значений, а `retry(n)`
повторно подписывается на издателя при ошибке:

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

## Отладка и наблюдение

<!-- example: operators/print-handle-events -->

Оператор `print` пишет каждое событие с префиксом, а `handleEvents`
запускает хуки на каждой стадии, не изменяя поток:

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

Оператор `breakpoint(receiveOutput:)` посылает сигнал `SIGTRAP`,
когда замыкание возвращает `true`; это инструмент отладчика, а не
оператор потока.

## Издаваемые по подключению издатели

<!-- example: operators/share -->

Оператор `share()` (комбинация multicast и autoconnect) позволяет
нескольким подписчикам разделить одну подписку:

```swift
let source = PassthroughSubject<Int, Never>()
let shared = source.share()
var first: [Int] = []
var second: [Int] = []

_ = shared.sink { first.append($0) }
_ = shared.sink { second.append($0) }

source.send(1)
source.send(2)
// first == [1, 2], second == [1, 2] — одна подписка, два наблюдателя.
```

<!-- example: operators/make-connectable -->

Оператор `makeConnectable()` откладывает поток до вызова
`connect()`: сначала подписываются все наблюдатели, и только затем
поток запускается:

```swift
let subject = PassthroughSubject<Int, Never>()
let connectable = subject.makeConnectable()
var seen: [Int] = []
_ = connectable.sink { seen.append($0) }

subject.send(1)      // подключения ещё нет — значение отброшено
let connection = connectable.connect()
subject.send(2)      // seen == [2]
connection.cancel()
```

Оператор `multicast(subject:)` обобщает этот принцип на любой
субъект, а `autoconnect()` выполняет подключение при первой подписке
и отменяет поток, когда отменяется последний подписчик.

## Кодирование

<!-- example: operators/encode-decode -->

Операторы `encode(encoder:)` и `decode(type:decoder:)` превращают
поток значений `Codable` в поток данных `Data` и обратно:

```swift
struct Article: Codable { let title: String; let year: Int }
let article = Article(title: "A", year: 2026)

var encoded: [Data] = []
_ = [article].publisher
    .encode(encoder: JSONEncoder())          // Failure == Error
    .sink(receiveCompletion: { _ in },
          receiveValue: { encoded.append($0) })
// encoded == [непустые JSON-данные статьи]

var decoded: [Article] = []
_ = encoded.publisher
    .decode(type: Article.self, decoder: JSONDecoder())
    .sink(receiveCompletion: { _ in },
          receiveValue: { decoded.append($0) })
// decoded == [Article(title: "A", year: 2026)] — круговой обход цел.
```

Оператор `replaceNil(with:)` — сахар над `Map` для потоков со
значениями-опционалами.

## Операторы времени

Операторы `debounce`, `throttle`, `delay`, `timeout`,
`measureInterval` и временной `collect(strategy:)` выполняются на
[планировщике](SCHEDULERS.md); детерминированные приёмы их тестирования
описаны там же.

## Полный список

Сгенерированный справочник `docs/API-REFERENCE.md` (английское
издание) перечисляет каждый тип оператора и подпись метода; группа
`Publishers` содержит полный набор реализаций операторов.
