import XCTest
import LinuxCombine
import Dispatch

/// Executable counterparts of every `<!-- example: getting-started/… -->`
/// snippet in docs/GETTING-STARTED.md (and the Russian edition).
final class GettingStartedExamplesTests: XCTestCase {

    func testGettingStartedHelloSink() {
        var received: [Int] = []
        let cancellable = [1, 2, 3].publisher
            .map { $0 * 10 }
            .sink { received.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(received, [10, 20, 30])
    }

    func testGettingStartedJustAndEmpty() {
        var values: [String] = []
        var completions: [Subscribers.Completion<Never>] = []

        let just = Just("hello").sink(
            receiveCompletion: { completions.append($0) },
            receiveValue: { values.append($0) }
        )
        defer { just.cancel() }

        XCTAssertEqual(values, ["hello"])
        XCTAssertEqual(completions.count, 1)
        if case .finished = completions[0] {} else {
            XCTFail("expected .finished")
        }

        let empty = Empty<Int, Never>().sink { _ in
            XCTFail("no value expected")
        }
        empty.cancel()
    }

    func testGettingStartedSequencePublisher() {
        let numbers = [10, 20, 30].publisher

        var seen: [Int] = []
        let cancellable = numbers.sink { seen.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(seen, [10, 20, 30])
    }

    func testGettingStartedSubject() {
        let subject = PassthroughSubject<Int, Never>()

        var received: [Int] = []
        let cancellable = subject.sink { received.append($0) }
        defer { cancellable.cancel() }

        subject.send(1)
        subject.send(2)
        subject.send(completion: .finished)
        subject.send(3)          // ignored — already finished

        XCTAssertEqual(received, [1, 2])
    }

    func testGettingStartedCurrentvalue() {
        let counter = CurrentValueSubject<Int, Never>(0)
        counter.send(5)

        var seen: [Int] = []
        let cancellable = counter.sink { seen.append($0) }
        defer { cancellable.cancel() }
        XCTAssertEqual(seen, [5], "the retained value arrives first")

        counter.send(6)
        XCTAssertEqual(seen, [5, 6])
    }

    func testGettingStartedAssign() {
        final class Model { var title = "" }
        let model = Model()

        let subject = PassthroughSubject<String, Never>()
        let cancellable = subject.assign(to: \.title, on: model)
        defer { cancellable.cancel() }

        subject.send("hello")
        XCTAssertEqual(model.title, "hello")
    }

    func testGettingStartedFuture() {
        let future = Future<Int, Never> { promise in
            promise(.success(42))
        }

        var seen: [Int] = []
        let cancellable = future.sink { seen.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(seen, [42])
    }

    func testGettingStartedCatch() {
        struct NotFound: Error {}

        let failing = Fail<Int, Error>(error: NotFound())

        var seen: [Int] = []
        let cancellable = failing
            .catch { _ in Just(-1) }
            .sink { seen.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(seen, [-1])
    }
}
