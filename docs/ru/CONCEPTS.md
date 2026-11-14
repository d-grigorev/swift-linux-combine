# Основные понятия

Эта страница посвящена механизму, лежащему под любым конвейером:
спросу, отмене, завершениям, идентификаторам, стиранию типов и модели
потоков выполнения. Каждый пример компилируется и выполняется в
`Tests/DocumentationExamplesTests`.

## Спрос (backpressure)

Издатели никогда не передают значений больше, чем запросил
подписчик. Спрос представлен типом `Subscribers.Demand` и принимает
значения `.none`, `.max(n)` или `.unlimited`; он суммируется и
действует накопительно.

<!-- example: concepts/demand-gating -->

Субъект уважает спрос: пока спроса нет, значения отбрасываются.
Оператор `sink` запрашивает `.unlimited`, поэтому в повседневном
коде это незаметно, но при собственном подписчике разница видна
напрямую:

```swift
let subject = PassthroughSubject<Int, Never>()

let subscriber = ManualDemandSubscriber<Int, Never>()
subject.subscribe(subscriber)

subject.send(1)                 // спроса нет — отброшено
subscriber.request(.max(2))     // запросить два значения
subject.send(2)
subject.send(3)
subject.send(4)                 // спрос исчерпан — отброшено
// subscriber.values == [2, 3]
```

Арифметика спроса ведёт себя ожидаемым образом:

```swift
let demand: Subscribers.Demand = .max(3) + .max(5)      // .max(8)
let unlimited: Subscribers.Demand = .unlimited + .max(5)  // .unlimited
let remainder: Subscribers.Demand = .max(5) - 3         // .max(2)
```

Отдельные операторы преобразуют спрос. Например, `collect(n)` при
`n == 3` превращает каждый `.max(n)` вниз по потоку в `.max(3 * n)`
вверх по потоку, поскольку каждый передаваемый массив потребляет три
элемента. Операторы семейства reduce (`allSatisfy`, `count`,
`min`/`max` и другие) при первом запросе запрашивают `.unlimited`
вверх по потоку и удерживают свой единственный результат до тех пор,
пока запрос не поступит.

## Завершения и отмена

<!-- example: concepts/completion -->

Поток закрывается ровно один раз, одной из веток перечисления
`Subscribers.Completion`:

```swift
enum ExampleError: Error { case boom }

var completion: Subscribers.Completion<ExampleError>?
var received: [Int] = []
_ = Fail<Int, ExampleError>(error: .boom)
    .sink(
        receiveCompletion: { completion = $0 },
        receiveValue: { received.append($0) }
    )
// received == []; completion == .failure(.boom) — завершения сравнивают
// сопоставлением образцов, а не по текстовому описанию: описания
// содержат имя модуля и зависят от окружения.
```

После завершения — или после `cancel()` — подписка неактивна: значений
больше не будет, второе завершение тоже не придёт.

<!-- example: concepts/anycancellable -->

`AnyCancellable` выполняет в этой модели роль держателя ресурса:
при уничтожении он отменяет подписку, если вы не сделали этого
явно, и хранится в привычных коллекциях:

```swift
var bag = Set<AnyCancellable>()
let subject = PassthroughSubject<Int, Never>()

var received: [Int] = []
subject.sink { received.append($0) }.store(in: &bag)

subject.send(1)
bag.removeAll()                 // отменяет все сохранённые подписки
subject.send(2)
// received == [1]
```

## Идентификаторы `combineIdentifier`

Каждый подписчик и каждая подписка несут стабильный идентификатор
`combineIdentifier`, по которому операторы вроде `combineLatest`
различают потоки, а тесты фиксируют равенство объектов. Собственные
типы подключают к этой механике протоколом
`CustomCombineIdentifierConvertible`.

## Стирание типов

<!-- example: concepts/erasure -->

Конкретный тип конвейера быстро становится неудобным; стирание типов
обменивает небольшую аллокацию в куче на непрозрачное имя:

```swift
let source: AnyPublisher<Int, Never> = [1, 2, 3].publisher.eraseToAnyPublisher()
let token: AnyCancellable = Just(1).sink { _ in }
```

`AnyPublisher` просто оборачивает конвейер, ничего в нём не меняя.

## Модель потоков выполнения

Библиотека потокобезопасна при соблюдении документированного контракта:

- **Субъекты и многоподписчицкие операторы** (`PassthroughSubject`,
  `Publishers.Merge`, `CombineLatest`, `Zip`) защищены внутренней
  блокировкой; конкурентные `send` и `subscribe` допустимы.
- **Одноразовые издатели значений** (`Just`, `Optional.Publisher`,
  `Result.Publisher`) сознательно **не используют блокировок**: их
  внутренний объект хранит подписчика в обычном необязательном поле,
  которое обнуляется при первой выдаче. При соблюдении контракта
  (один логический производитель) это свободно от гонок; такой дизайн
  закреплён тестами жизненного цикла.
- **Переход на другой поток всегда явный**: оператор `receive(on:)`
  переносит доставку через планировщик; ничего не переносится
  неявно. Подробности — в [SCHEDULERS.md](SCHEDULERS.md).

<!-- example: concepts/thread-safe-subject -->

Конкурентная отправка в субъект из нескольких потоков безопасна;
порядок доставки конкретному подписчику определяется тем, как
блокировки сериализуют вызовы:

```swift
let subject = PassthroughSubject<Int, Never>()
var received: [Int] = []
let cancellable = subject.sink { received.append($0) }

DispatchQueue.concurrentPerform(iterations: 100) { i in
    subject.send(i)
}
cancellable.cancel()
// received.count == 100 — каждое значение доставлено ровно один раз.
```

## Однократность и многоподписчицкость

Издателю можно подписываться многократно: каждая подписка получает
независимый поток. Два исключения — `Future` (один общий
запомненный результат) и `Share` (одна общая подписка; см.
[OPERATORS.md](OPERATORS.md)).

## Одна ловушка на заметку

Операторы, вызывающие пользовательские замыкания, никогда не держат
внутренние блокировки во время этих вызовов — ваши замыкания могут
безопасно обращаться к конвейеру заново: запрашивать спрос, отменять
подписку и даже подписываться снова. Это закреплено набором тестов
паритета и объясняет, почему хуки `handleEvents` допускают повторный
вход в конвейер.
