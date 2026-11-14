# Совместимость с async/await

Потоки Combine можно потреблять напрямую конструкциями
`for await … in`: свойство `values` оборачивает любой издатель в
асинхронную последовательность (`AsyncSequence`).

## Потоки без ошибок (`Failure == Never`)

<!-- example: async-await/basic-values -->

```swift
let sequence = [1, 2, 3, 4].publisher    // Publishers.Sequence<Int, Never>

var collected: [Int] = []
for await value in sequence.values {
    collected.append(value)
}
// collected == [1, 2, 3, 4]; цикл завершается вместе с издателем.
```

## Потоки с ошибками

<!-- example: async-await/throwing-values -->

Для любого другого типа ошибки `values` даёт
`AsyncThrowingPublisher`: цикл становится `for try await`, а ошибка
издателя бросается прямо в точке итерации:

```swift
enum TestingDocsError: Error { case sample }

let failing = Fail<Int, TestingDocsError>(error: .sample)

var received: [Int] = []
do {
    for try await value in failing.values {
        received.append(value)
    }
    XCTFail("ожидалось, что ошибка будет брошена")
} catch {
    // error == TestingDocsError.sample
}
// received == []
```

## По одному значению за раз

<!-- example: async-await/iterator -->

Итератор запрашивает `.max(1)` на каждый вызов `next()` — спрос
ровно настолько ленив, насколько ленив ваш цикл:

```swift
let subject = CurrentValueSubject<Int, Never>(42)
var iterator = subject.values.makeAsyncIterator()

// Значение, отправленное до любого next(), не доставляется: насос
// запрашивает .max(1) только на каждый вызов next().
let value = await iterator.next()
// value == 42
```

## Отмена

Итерация в `Task`, который отменяют, отменяет подписку вверх по
потоку: насос устанавливает обработчик отмены вокруг каждого
`next()`:

```swift
let subject = PassthroughSubject<Int, Never>()
var iterator = subject.values.makeAsyncIterator()

let task = Task { await iterator.next() }
task.cancel()
// Подписка вверх по потоку отменена; next() вернёт nil.
```

## Совмещение парадигм

Мосты работают в обе стороны:

- из async в Combine: оберните результат ожидания в `Future` либо
  отправляйте значения в `PassthroughSubject` из `Task`;
- из Combine в async: цикл `values` выше или вспомогательный
  объект-континуация вокруг любого подписчика.

Предпочтительно выбирать одну парадигму на конвейер и соединять их
на его границе.

## Доступность

Типы `AsyncPublisher` и `AsyncThrowingPublisher` помечены
аннотацией `@available(macOS 10.15, iOS 13.0, watchOS 6.0,
tvOS 13.0, *)` — на Linux она не имеет значения, но требуется для
исходной совместимости с Apple-платформами.
