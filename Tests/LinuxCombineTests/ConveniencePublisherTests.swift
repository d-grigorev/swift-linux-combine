import XCTest
@testable import LinuxCombine

private struct NeverError: Error, Equatable {}

final class JustTests: XCTestCase {

    func testValueAndCompletionDeliveredOnFirstRequest() {
        let just = Just(7)
        let sub = TestSubscriber<Int, Never>()
        just.subscribe(sub)

        XCTAssertEqual(sub.values, [7])
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testNothingDeliveredBeforeRequest() {
        let just = Just(7)
        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        just.subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.values, [])

        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [7])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testCancelBeforeRequestDeliversNothing() {
        let just = Just(7)
        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        just.subscribe(sub)
        sub.firstSubscription?.cancel()
        sub.firstSubscription?.request(.max(1))

        XCTAssertEqual(sub.values, [])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testSecondRequestAfterCompletionIsIgnored() {
        let just = Just(7)
        let sub = TestSubscriber<Int, Never>()
        just.subscribe(sub)
        XCTAssertEqual(sub.values, [7])

        sub.firstSubscription?.request(.max(5))
        XCTAssertEqual(sub.values, [7])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testEachSubscriberGetsTheValue() {
        let just = Just(1)
        let a = TestSubscriber<Int, Never>()
        let b = TestSubscriber<Int, Never>()
        just.subscribe(a)
        just.subscribe(b)
        XCTAssertEqual(a.values, [1])
        XCTAssertEqual(b.values, [1])
    }

    func testAppendPrependBuildSequencePublishers() {
        var collected: [Int] = []
        var finished = false
        Just(2)
            .append(3, 4)
            .subscribe(AnySubscriber(
                receiveSubscription: { $0.request(.unlimited) },
                receiveValue: { collected.append($0); return .none },
                receiveCompletion: { if case .finished = $0 { finished = true } }
            ))
        XCTAssertEqual(collected, [2, 3, 4])
        XCTAssertTrue(finished)

        var prepended: [Int] = []
        Just(2)
            .prepend(0, 1)
            .subscribe(AnySubscriber(
                receiveSubscription: { $0.request(.unlimited) },
                receiveValue: { prepended.append($0); return .none }
            ))
        XCTAssertEqual(prepended, [0, 1, 2])
    }

    func testEquatable() {
        XCTAssertEqual(Just(1), Just(1))
        XCTAssertNotEqual(Just(1), Just(2))
    }
}

final class EmptyTests: XCTestCase {

    func testCompletesImmediatelyWithoutDemand() {
        let sub = TestSubscriber<Int, Never>()
        Empty<Int, Never>().subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testNeverCompletesWhenConfiguredNotTo() {
        let sub = TestSubscriber<Int, Never>()
        Empty<Int, Never>(completeImmediately: false).subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertTrue(sub.completions.isEmpty)

        // Requests go to Subscriptions.empty: no-ops, no crash.
        sub.firstSubscription?.request(.max(3))
        sub.firstSubscription?.cancel()
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testTypedInitializers() {
        let typed = Empty<Int, NeverError>(completeImmediately: true, outputType: Int.self, failureType: NeverError.self)
        let sub = TestSubscriber<Int, NeverError>()
        typed.subscribe(sub)
        XCTAssertEqual(sub.completions.count, 1)
        XCTAssertEqual(typed.completeImmediately, true)
    }

    func testEquatable() {
        XCTAssertEqual(Empty<Int, Never>(), Empty<Int, Never>())
        XCTAssertNotEqual(
            Empty<Int, Never>(completeImmediately: true),
            Empty<Int, Never>(completeImmediately: false)
        )
    }
}

final class FailTests: XCTestCase {

    func testImmediateFailureWithoutDemand() {
        let sub = TestSubscriber<Int, NeverError>()
        Fail<Int, NeverError>(error: NeverError()).subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.completions.count, 1)
        guard case .failure(let error) = sub.completions[0] else {
            return XCTFail("expected failure")
        }
        XCTAssertEqual(error, NeverError())
    }

    func testOutputTypeInitializer() {
        let fail = Fail<Int, NeverError>(outputType: Int.self, failure: NeverError())
        XCTAssertEqual(fail.error, NeverError())
        let sub = TestSubscriber<Int, NeverError>()
        fail.subscribe(sub)
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testEquatableWhereFailureEquatable() {
        XCTAssertEqual(Fail<Int, NeverError>(error: NeverError()),
                       Fail<Int, NeverError>(error: NeverError()))
    }
}

final class DeferredTests: XCTestCase {

    func testFactoryRunsPerSubscriberNotAtInit() {
        var created = 0
        let deferred = Deferred {
            created += 1
            return Just(created)
        }
        XCTAssertEqual(created, 0, "factory must not run at init")

        let a = TestSubscriber<Int, Never>()
        deferred.subscribe(a)
        XCTAssertEqual(created, 1)
        XCTAssertEqual(a.values, [1])

        let b = TestSubscriber<Int, Never>()
        deferred.subscribe(b)
        XCTAssertEqual(created, 2)
        XCTAssertEqual(b.values, [2])
    }
}

final class SequencePublisherTests: XCTestCase {

    func testPublisherPropertyDeliversAllWithUnlimitedDemand() {
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        [1, 2, 3].publisher.subscribe(sub)

        XCTAssertEqual(sub.values, [1, 2, 3])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testDemandGatedDeliveryAndCompletion() {
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(2)
        [1, 2, 3].publisher.subscribe(sub)

        // First request: two elements; the completion still awaits demand.
        XCTAssertEqual(sub.values, [1, 2])
        XCTAssertTrue(sub.completions.isEmpty)

        // Delivering the last element completes immediately after it.
        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [1, 2, 3])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testReentrantRequestChainsDeliveryFromOneRequest() {
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(1)
        sub.additionalDemandPerValue = .max(1)
        [10, 20, 30].publisher.subscribe(sub)

        // One initial request is enough: each delivery requests the next.
        XCTAssertEqual(sub.values, [10, 20, 30])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testEmptySequenceCompletesImmediately() {
        let sub = TestSubscriber<Int, Never>()
        [Int]().publisher.subscribe(sub)
        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.completions.count, 1)
        XCTAssertTrue(sub.values.isEmpty)
    }

    func testCancelStopsDelivery() {
        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        [1, 2, 3].publisher.subscribe(sub)
        sub.firstSubscription?.cancel()
        sub.firstSubscription?.request(.max(3))
        XCTAssertTrue(sub.values.isEmpty)
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testEquatable() {
        XCTAssertEqual([1, 2].publisher, [1, 2].publisher)
        XCTAssertNotEqual([1, 2].publisher, [1, 3].publisher)
    }

    func testAppendPrependRequireRangeReplaceableCollection() {
        var appended: [Int] = []
        [2, 3].publisher.append(4).subscribe(AnySubscriber(
            receiveSubscription: { $0.request(.unlimited) },
            receiveValue: { appended.append($0); return .none }
        ))
        XCTAssertEqual(appended, [2, 3, 4])

        var prepended: [Int] = []
        [2, 3].publisher.prepend([0, 1]).subscribe(AnySubscriber(
            receiveSubscription: { $0.request(.unlimited) },
            receiveValue: { prepended.append($0); return .none }
        ))
        XCTAssertEqual(prepended, [0, 1, 2, 3])
    }

    func testMultipleSubscribersIterateIndependently() {
        let source = [1, 2, 3]
        let a = TestSubscriber<Int, Never>()
        let b = TestSubscriber<Int, Never>()
        a.initialDemand = .unlimited
        b.initialDemand = .unlimited
        source.publisher.subscribe(a)
        source.publisher.subscribe(b)
        XCTAssertEqual(a.values, [1, 2, 3])
        XCTAssertEqual(b.values, [1, 2, 3])
    }
}
