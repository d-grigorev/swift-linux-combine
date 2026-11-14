import XCTest
@testable import LinuxCombine

/// Own tests for `Publishers.CombineLatest` (OpenCombine ships no
/// oracle for it — documented choices per docs/QUIRKS.md §B, pinned
/// here): emits after every source has produced at least one value,
/// pairing the newest values of all sources; finishes when ALL
/// upstreams finish; the first failure terminates immediately and
/// cancels the siblings' upstream subscriptions.
final class CombineLatestTests: XCTestCase {

    func testEmitsAfterAllSourcesHaveValues() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<String, Never>()
        let sub = TestSubscriber<(Int, String), Never>()
        sub.initialDemand = .unlimited
        a.combineLatest(b).subscribe(sub)

        a.send(1)
        XCTAssertEqual(sub.values.count, 0, "b has no value yet")
        b.send("x")
        XCTAssertEqual(sub.values.count, 1)
        XCTAssertEqual(sub.values[0].0, 1)
        XCTAssertEqual(sub.values[0].1, "x")

        a.send(2)      // newest a with stored b
        XCTAssertEqual(sub.values.count, 2)
        XCTAssertEqual(sub.values[1].0, 2)
        XCTAssertEqual(sub.values[1].1, "x")

        b.send("y")    // newest b with stored a
        XCTAssertEqual(sub.values.count, 3)
        XCTAssertEqual(sub.values[2].0, 2)
        XCTAssertEqual(sub.values[2].1, "y")
    }

    func testFinishesWhenAllUpstreamsFinish() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<String, Never>()
        let sub = TestSubscriber<(Int, String), Never>()
        sub.initialDemand = .unlimited
        a.combineLatest(b).subscribe(sub)

        a.send(1)
        b.send("x")
        a.send(completion: .finished)
        XCTAssertEqual(sub.completions.count, 0, "b is still live")

        b.send(completion: .finished)
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testFirstFailureTerminatesAndCancelsSiblings() {
        struct Boom: Error {}

        let a = PassthroughSubject<Int, Error>()
        let b = PassthroughSubject<String, Error>()
        let sub = TestSubscriber<(Int, String), Error>()
        sub.initialDemand = .unlimited
        a.combineLatest(b).subscribe(sub)

        a.send(1)
        b.send("x")
        a.send(completion: .failure(Boom()))
        XCTAssertEqual(sub.completions.count, 1)
        if case .failure = sub.completions[0] {} else {
            XCTFail("expected failure")
        }

        // The stream is over: further sibling activity is ignored.
        b.send("y")
        XCTAssertEqual(sub.values.count, 1)
    }

    func testWithTransformClosure() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        a.combineLatest(b, +).subscribe(sub)

        a.send(1)
        b.send(10)
        a.send(2)
        b.send(20)

        XCTAssertEqual(sub.values, [11, 12, 22])
    }
}
