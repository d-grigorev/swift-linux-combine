import XCTest
import LinuxCombine
import Dispatch

/// Executable counterparts of every `<!-- example: schedulers/… -->`
/// snippet in docs/SCHEDULERS.md (and the Russian edition).
final class SchedulersExamplesTests: XCTestCase {

    func testSchedulersImmediate() {
        let subject = PassthroughSubject<Int, Never>()
        var seen: [Int] = []
        let cancellable = subject
            .delay(for: .seconds(5), scheduler: ImmediateScheduler.shared)
            .sink { seen.append($0) }
        defer { cancellable.cancel() }

        subject.send(1)
        // No time actually passes; the delay is a no-op.
        XCTAssertEqual(seen, [1])
    }

    func testSchedulersDispatchqueue() {
        let subject = PassthroughSubject<Int, Never>()
        var seen: [Int] = []
        let queue = DispatchQueue(label: "examples.delivery")

        let cancellable = subject
            .receive(on: queue)
            .sink { seen.append($0) }
        defer { cancellable.cancel() }

        subject.send(1)
        queue.sync {}        // wait for the hop
        XCTAssertEqual(seen, [1])
    }

    func testSchedulersReceiveOn() {
        let queue = DispatchQueue(label: "examples.hop")
        let subject = PassthroughSubject<Int, Never>()
        let firstThread = Thread.current

        var deliveryThread: Thread?
        let expectation = expectation(description: "delivered")
        let cancellable = subject
            .receive(on: queue)
            .sink { _ in
                deliveryThread = Thread.current
                expectation.fulfill()
            }
        defer { cancellable.cancel() }

        subject.send(1)
        wait(for: [expectation], timeout: 5)

        XCTAssertNotNil(deliveryThread)
        XCTAssertFalse(deliveryThread === firstThread)
    }
}
