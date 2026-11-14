import XCTest
import LinuxCombine
import Dispatch

/// Executable counterparts of every `<!-- example: concepts/… -->`
/// snippet in docs/CONCEPTS.md (and the Russian edition).
final class ConceptsExamplesTests: XCTestCase {

    func testConceptsDemandGating() {
        let subject = PassthroughSubject<Int, Never>()

        let subscriber = ManualDemandSubscriber<Int, Never>()
        subject.subscribe(subscriber)

        subject.send(1)                 // no demand yet → dropped
        subscriber.request(.max(2))     // ask for two
        subject.send(2)
        subject.send(3)
        subject.send(4)                 // demand exhausted → dropped

        XCTAssertEqual(subscriber.values, [2, 3])
        XCTAssertTrue(subscriber.completions.isEmpty)

        // Demand arithmetic is additive/cumulative:
        let demand: Subscribers.Demand = .max(3) + .max(5)
        XCTAssertEqual(demand, .max(8))
        let unlimited: Subscribers.Demand = .unlimited + .max(5)
        XCTAssertEqual(unlimited, .unlimited)
        let remainder: Subscribers.Demand = .max(5) - 3
        XCTAssertEqual(remainder, .max(2))
    }

    func testConceptsCompletion() {
        var completion: Subscribers.Completion<DocsExampleError>?
        var received: [Int] = []
        let cancellable = Fail<Int, DocsExampleError>(error: .boom)
            .sink(
                receiveCompletion: { completion = $0 },
                receiveValue: { received.append($0) }
            )
        defer { cancellable.cancel() }

        XCTAssertTrue(received.isEmpty)
        if case .failure(.boom) = completion {
            // failure delivered, stream over
        } else {
            XCTFail("expected .failure(.boom), got \(String(describing: completion))")
        }
    }

    func testConceptsAnycancellable() {
        var bag = Set<AnyCancellable>()
        let subject = PassthroughSubject<Int, Never>()

        var received: [Int] = []
        subject.sink { received.append($0) }.store(in: &bag)

        subject.send(1)
        bag.removeAll()                 // cancels everything
        subject.send(2)

        XCTAssertEqual(received, [1])
    }

    func testConceptsErasure() {
        let source: AnyPublisher<Int, Never> =
            [1, 2, 3].publisher.eraseToAnyPublisher()
        var seen: [Int] = []
        let token: AnyCancellable = source.sink { seen.append($0) }
        defer { token.cancel() }

        XCTAssertEqual(seen, [1, 2, 3])
    }

    func testConceptsThreadSafeSubject() {
        let subject = PassthroughSubject<Int, Never>()
        var received: [Int] = []
        let cancellable = subject.sink { received.append($0) }
        defer { cancellable.cancel() }

        DispatchQueue.concurrentPerform(iterations: 100) { i in
            subject.send(i)
        }
        // Per-subscriber delivery is serialized by the subject — every
        // value is delivered exactly once.
        XCTAssertEqual(received.count, 100)
        XCTAssertEqual(Set(received).count, 100)
    }
}

// Internal (not private): `String(describing:)` prints the plain name.
enum DocsExampleError: Error { case boom }
