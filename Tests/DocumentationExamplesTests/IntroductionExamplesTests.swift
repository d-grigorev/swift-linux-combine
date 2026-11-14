import XCTest
import LinuxCombine

enum IntroParsingError: Error { case malformed }

/// Executable counterparts of every `<!-- example: intro/… -->`
/// snippet in docs/ru/INTRO.md (the introductory chapter for readers
/// new to reactive programming).
final class IntroductionExamplesTests: XCTestCase {

    func testIntroWhyReactive() {
        // Императивный стиль: каждое событие — отдельный вызов замыкания.
        final class Counter {
            var onChange: ((Int) -> Void)?
            private(set) var value = 0

            func increment() {
                value += 1
                onChange?(value)
            }
        }

        let counter = Counter()
        var calls: [Int] = []
        counter.onChange = { value in
            calls.append(value)
        }
        counter.increment()

        XCTAssertEqual(calls, [1], "обработчик вызван один раз со значением 1")
    }

    func testIntroPublisherSubscriber() {
        let subject = PassthroughSubject<Int, Never>()

        var received: [Int] = []
        let token = subject.sink { received.append($0) }

        subject.send(1)
        subject.send(2)
        XCTAssertEqual(received, [1, 2])

        token.cancel()
        subject.send(3)
        XCTAssertEqual(received, [1, 2], "после отмены значения не поступают")
    }

    func testIntroSubscriptionCancel() {
        var bag = Set<AnyCancellable>()
        let subject = PassthroughSubject<String, Never>()

        var received: [String] = []
        subject.sink { received.append($0) }.store(in: &bag)

        subject.send("a")
        bag.removeAll()          // отмена всех сохранённых подписок
        subject.send("b")

        XCTAssertEqual(received, ["a"])
    }

    func testIntroDemandBasics() {
        let subject = PassthroughSubject<Int, Never>()

        let subscriber = ManualDemandSubscriber<Int, Never>()
        subject.subscribe(subscriber)

        subject.send(1)              // спроса нет — значение отброшено
        subscriber.request(.max(2))  // теперь спрос равен двум значениям
        subject.send(2)
        subject.send(3)
        subject.send(4)              // спрос исчерпан — значение отброшено

        XCTAssertEqual(subscriber.values, [2, 3])
    }

    func testIntroCompletionVsEmpty() {
        var events: [String] = []

        let empty = Empty<Int, Never>().sink(
            receiveCompletion: { _ in events.append("finished") },
            receiveValue: { _ in events.append("value") }
        )
        empty.cancel()

        XCTAssertEqual(events, ["finished"],
                       "завершение пришло, значений не было")
    }

    func testIntroErrorStream() {
        var results: [String] = []

        let failing = Fail<Int, IntroParsingError>(error: .malformed)
            .catch { _ in Just(-1) }          // заменить поток резервным
            .sink { results.append("\($0)") }
        defer { failing.cancel() }

        XCTAssertEqual(results, ["-1"],
                       "ошибка перехвачена и заменена значением")
    }

    func testIntroCompareIterator() async {
        var iterator = [1, 2, 3].publisher.values.makeAsyncIterator()
        // Асинхронное получение: каждое next() запрашивает одно значение.
        var collected: [Int] = []
        while let value = await iterator.next() {
            collected.append(value)
        }

        XCTAssertEqual(collected, [1, 2, 3])
    }

    func testIntroCombineWithPlain() {
        let numbers = [1, 2, 3, 4].publisher   // коллекция -> издатель

        var squares: [Int] = []
        let token = numbers
            .map { $0 * $0 }                   // декларативное преобразование
            .sink { squares.append($0) }       // возврат к обычному коду
        token.cancel()

        XCTAssertEqual(squares, [1, 4, 9, 16])
    }
}
