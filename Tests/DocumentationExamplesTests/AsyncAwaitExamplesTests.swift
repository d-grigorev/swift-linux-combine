import XCTest
import LinuxCombine

enum TestingDocsError: Error { case sample }

/// Executable counterparts of every `<!-- example: async-await/… -->`
/// snippet in docs/ASYNC-AWAIT.md (and the Russian edition).
final class AsyncAwaitExamplesTests: XCTestCase {

    func testAsyncAwaitBasicValues() async {
        let sequence = [1, 2, 3, 4].publisher

        var collected: [Int] = []
        for await value in sequence.values {
            collected.append(value)
        }

        XCTAssertEqual(collected, [1, 2, 3, 4])
    }

    func testAsyncAwaitThrowingValues() async {
        let failing = Fail<Int, TestingDocsError>(error: .sample)

        var received: [Int] = []
        do {
            for try await value in failing.values {
                received.append(value)
            }
            XCTFail("expected the failure to throw")
        } catch {
            guard case TestingDocsError.sample = error else {
                XCTFail("unexpected error: \(error)")
                return
            }
        }

        XCTAssertEqual(received, [])
    }

    func testAsyncAwaitIterator() async {
        let subject = CurrentValueSubject<Int, Never>(42)
        var iterator = subject.values.makeAsyncIterator()

        // A value sent before any next() is not delivered: the pump only
        // requests .max(1) per next() call.
        let value = await iterator.next()
        XCTAssertEqual(value, 42)
    }
}
