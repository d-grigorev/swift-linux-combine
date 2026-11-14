import XCTest
@testable import LinuxCombine

private struct TestFailure: Error, Equatable {}

final class PassthroughSubjectTests: XCTestCase {

    func testDeliversOnDemandAndDropsWithoutDemand() {
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(1)
        subject.subscribe(sub)

        subject.send(1)   // demand 1 -> delivered
        subject.send(2)   // demand 0 -> dropped (documented behavior)
        subject.send(3)   // still dropped

        XCTAssertEqual(sub.values, [1])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testExtraDemandReturnedFromReceiveIsHonored() {
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(1)
        sub.additionalDemandPerValue = .max(1)
        subject.subscribe(sub)

        subject.send(1)   // delivered, +1 extra demand
        subject.send(2)   // delivered
        subject.send(3)   // delivered

        XCTAssertEqual(sub.values, [1, 2, 3])
    }

    func testMultipleSubscribersReceiveIndependently() {
        let subject = PassthroughSubject<Int, Never>()
        let first = TestSubscriber<Int, Never>()
        let second = TestSubscriber<Int, Never>()
        first.initialDemand = .max(1)
        second.initialDemand = .max(2)
        subject.subscribe(first)
        subject.subscribe(second)

        subject.send(1)
        subject.send(2)

        XCTAssertEqual(first.values, [1])   // demand 1 -> second value dropped
        XCTAssertEqual(second.values, [1, 2])
    }

    func testCompletionDeliveredAndReplayedToLateSubscribers() {
        let subject = PassthroughSubject<Int, TestFailure>()
        let early = TestSubscriber<Int, TestFailure>()
        subject.subscribe(early)
        subject.send(1)
        subject.send(completion: .failure(TestFailure()))

        XCTAssertEqual(early.completions.count, 1)
        guard case .failure = early.completions[0] else {
            return XCTFail("expected failure completion")
        }

        // Late subscriber: empty subscription + replayed completion.
        let late = TestSubscriber<Int, TestFailure>()
        subject.subscribe(late)
        XCTAssertEqual(late.subscriptionCount, 1)
        XCTAssertEqual(late.completions.count, 1)
        guard case .failure = late.completions[0] else {
            return XCTFail("expected replayed failure completion")
        }

        // Sends after completion are ignored everywhere.
        subject.send(99)
        subject.send(completion: .finished)
        XCTAssertEqual(early.values, [1])
        XCTAssertEqual(late.values, [])
        XCTAssertEqual(late.completions.count, 1)
    }

    func testCancelStopsDelivery() {
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        subject.subscribe(sub)
        sub.firstSubscription?.cancel()

        subject.send(1)
        XCTAssertEqual(sub.values, [])
    }

    func testReentrantSendFromReceiveDoesNotDeadlock() {
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        var hookFired = 0
        sub.onValue = { value in
            hookFired += 1
            if hookFired == 1 {
                subject.send(2)   // re-entrant send from inside receive(1)
            }
        }
        subject.subscribe(sub)

        subject.send(1)

        XCTAssertEqual(sub.values, [1, 2])
        XCTAssertEqual(hookFired, 2)
    }

    func testRelayedSubscriptionReceivesUnlimitedAfterDownstreamDemand() {
        let subject = PassthroughSubject<Int, Never>()
        let upstream = TestSubscription()

        // Subscription arrives before any downstream demand: no request yet.
        subject.send(subscription: upstream)
        XCTAssertTrue(upstream.requested.isEmpty)

        let sub = TestSubscriber<Int, Never>()
        subject.subscribe(sub)   // auto-requests .max(1) -> demand acknowledged
        XCTAssertEqual(upstream.requested, [.unlimited])

        // The latch means a second subscriber does not re-request.
        let second = TestSubscriber<Int, Never>()
        subject.subscribe(second)
        XCTAssertEqual(upstream.requested, [.unlimited])
    }

    func testSubjectDeinitCancelsRelayedUpstreams() {
        let upstream = TestSubscription()
        var subject: PassthroughSubject<Int, Never>? = PassthroughSubject()
        subject?.send(subscription: upstream)
        XCTAssertFalse(upstream.cancelled)
        subject = nil
        XCTAssertTrue(upstream.cancelled)
    }
}

final class CurrentValueSubjectTests: XCTestCase {

    func testCurrentValueDeliveredOnFirstRequest() {
        let subject = CurrentValueSubject<Int, Never>(42)
        let sub = TestSubscriber<Int, Never>()
        subject.subscribe(sub)

        XCTAssertEqual(sub.values, [42])
        XCTAssertEqual(subject.value, 42)
    }

    func testValueSetterPublishes() {
        let subject = CurrentValueSubject<Int, Never>(0)
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(2)
        subject.subscribe(sub)

        subject.value = 7
        XCTAssertEqual(subject.value, 7)
        XCTAssertEqual(sub.values, [0, 7])
    }

    func testDroppedSendIsRedeliveredAsCurrentOnNextRequest() {
        let subject = CurrentValueSubject<Int, Never>(0)
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(2)
        subject.subscribe(sub)
        // After the initial value (0), demand is 1.

        subject.send(1)   // delivered; demand drops to 0
        subject.send(2)   // dropped -> marks current value undelivered

        sub.firstSubscription?.request(.max(1))
        // Redelivers the *current* value (2), consuming the request.
        XCTAssertEqual(sub.values, [0, 1, 2])

        subject.send(3)   // demand 0 again -> dropped
        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [0, 1, 2, 3])
    }

    func testCompletionReplaysToLateSubscriber() {
        let subject = CurrentValueSubject<Int, TestFailure>(5)
        subject.send(completion: .finished)

        let late = TestSubscriber<Int, TestFailure>()
        subject.subscribe(late)
        XCTAssertEqual(late.subscriptionCount, 1)
        XCTAssertEqual(late.completions.count, 1)
        XCTAssertEqual(late.values, [])   // no value after completion

        // Sends after completion are ignored everywhere...
        subject.send(6)
        XCTAssertEqual(late.values, [])
        // ...but the value storage stays writable via the setter.
        subject.value = 6
        XCTAssertEqual(subject.value, 6)
    }

    func testSendSubscriptionAlwaysRequestsUnlimited() {
        // Unlike PassthroughSubject, CurrentValueSubject requests upstream
        // immediately, regardless of downstream demand.
        let subject = CurrentValueSubject<Int, Never>(0)
        let upstream = TestSubscription()
        subject.send(subscription: upstream)
        XCTAssertEqual(upstream.requested, [.unlimited])
    }

    func testReentrantSendFromReceiveDoesNotDeadlock() {
        let subject = CurrentValueSubject<Int, Never>(0)
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        var hookFired = 0
        sub.onValue = { _ in
            hookFired += 1
            if hookFired == 1 {
                subject.send(99)
            }
        }
        subject.subscribe(sub)   // delivers 0 first

        subject.send(1)

        XCTAssertEqual(sub.values, [0, 99, 1])
        XCTAssertEqual(subject.value, 1)
    }

    func testSubjectDeinitCancelsRelayedUpstreams() {
        let upstream = TestSubscription()
        var subject: CurrentValueSubject<Int, Never>? = CurrentValueSubject(0)
        subject?.send(subscription: upstream)
        XCTAssertFalse(upstream.cancelled)
        subject = nil
        XCTAssertTrue(upstream.cancelled)
    }
}
