import XCTest
@testable import LinuxCombine

/// Own tests for `Publishers.CollectByTime` + `TimeGroupingStrategy`.
///
/// No upstream oracle exists for these (OpenCombine doesn't implement
/// them — see docs/QUIRKS.md §B), so these pin the documented choices
/// from `Publishers/CollectByTime.swift`: unlimited upstream pull,
/// demand-gated emission with hold-until-request, empty windows emit
/// nothing, completion flushes the partial group, failures drop it.
/// Deterministic via the borrowed `VirtualTimeScheduler`.
final class CollectByTimeTests: XCTestCase {

    private func makeScheduler() -> VirtualTimeScheduler {
        VirtualTimeScheduler()
    }

    func testByTimeOrCountEmitsWhenCountReached() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .unlimited
        subject.collect(.byTimeOrCount(scheduler, .seconds(60), 3)).subscribe(sub)

        subject.send(1)
        subject.send(2)
        XCTAssertEqual(sub.values, [], "group incomplete before count is reached")

        subject.send(3)
        XCTAssertEqual(sub.values, [[1, 2, 3]], "count reached -> immediate group")

        subject.send(4)
        XCTAssertEqual(sub.values, [[1, 2, 3]])

        // Advance past the time window: the leftover group flushes.
        scheduler.rewind(to: scheduler.now.advanced(by: .seconds(90)))
        XCTAssertEqual(sub.values, [[1, 2, 3], [4]])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testByTimeGroupsOnWindowElapse() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .unlimited
        subject.collect(.byTime(scheduler, .seconds(1))).subscribe(sub)

        subject.send(1)
        subject.send(2)
        XCTAssertEqual(sub.values, [], "window still open")

        scheduler.rewind(to: scheduler.now.advanced(by: .seconds(2)))
        XCTAssertEqual(sub.values, [[1, 2]])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testEmptyWindowEmitsNothing() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .unlimited
        subject.collect(.byTime(scheduler, .seconds(1))).subscribe(sub)

        // No values were ever sent: nothing to group, no empty arrays.
        scheduler.rewind(to: scheduler.now.advanced(by: .seconds(5)))
        XCTAssertEqual(sub.values, [])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testGroupIsHeldUntilDemand() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .max(1)
        subject.collect(.byTimeOrCount(scheduler, .seconds(600), 3)).subscribe(sub)

        subject.send(1)
        subject.send(2)
        subject.send(3)   // count reached; demand is 1 -> emitted
        XCTAssertEqual(sub.values, [[1, 2, 3]])

        subject.send(4)
        subject.send(5)
        subject.send(6)   // count reached, but no demand -> held
        XCTAssertEqual(sub.values, [[1, 2, 3]], "held until requested")

        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [[1, 2, 3], [4, 5, 6]])
    }

    func testCompletionFlushesPartialGroup() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .unlimited
        subject.collect(.byTime(scheduler, .seconds(600))).subscribe(sub)

        subject.send(1)
        subject.send(2)
        subject.send(completion: .finished)

        XCTAssertEqual(sub.values, [[1, 2]])
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testEmptyCompletionEmitsOnlyFinished() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<[Int], Never>()
        sub.initialDemand = .unlimited
        subject.collect(.byTime(scheduler, .seconds(600))).subscribe(sub)

        subject.send(completion: .finished)

        XCTAssertEqual(sub.values, [])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testFailureDropsBuffer() {
        let scheduler = makeScheduler()
        let subject = PassthroughSubject<Int, GroupTestError>()
        let sub = TestSubscriber<[Int], GroupTestError>()
        sub.initialDemand = .unlimited
        subject.collect(.byTime(scheduler, .seconds(600))).subscribe(sub)

        subject.send(1)
        subject.send(2)
        subject.send(completion: .failure(.boom))

        XCTAssertEqual(sub.values, [], "failure drops the buffered group")
        XCTAssertEqual(sub.completions.count, 1)
        if case .failure(.boom) = sub.completions[0] {} else {
            XCTFail("expected failure")
        }
    }
}

private enum GroupTestError: Error {
    case boom
}
