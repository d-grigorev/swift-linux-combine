import XCTest
@testable import LinuxCombine

/// Own tests for `Publishers.Merge` (OpenCombine ships no oracle for
/// it — documented choices per docs/QUIRKS.md §B, pinned here):
/// interleaves the upstreams in arrival order; finishes when ALL
/// upstreams finish; the first failure terminates immediately and
/// cancels the siblings' upstream subscriptions.
final class MergeTests: XCTestCase {

    func testInterleavesArrivalOrder() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        Publishers.Merge(a, b).subscribe(sub)

        a.send(1)
        b.send(2)
        a.send(3)
        b.send(4)

        XCTAssertEqual(sub.values, [1, 2, 3, 4])
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testFinishesWhenAllUpstreamsFinish() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        Publishers.Merge(a, b).subscribe(sub)

        a.send(1)
        a.send(completion: .finished)
        XCTAssertEqual(sub.completions.count, 0, "b is still live")

        b.send(2)
        b.send(completion: .finished)
        XCTAssertEqual(sub.values, [1, 2])
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testFirstFailureTerminatesAndCancelsSiblings() {
        struct Boom: Error {}

        let a = PassthroughSubject<Int, Error>()
        let b = PassthroughSubject<Int, Error>()
        let sub = TestSubscriber<Int, Error>()
        sub.initialDemand = .unlimited
        Publishers.Merge(a, b).subscribe(sub)

        a.send(1)
        b.send(2)
        a.send(completion: .failure(Boom()))
        XCTAssertEqual(sub.completions.count, 1)
        if case .failure = sub.completions[0] {} else {
            XCTFail("expected failure")
        }

        b.send(3)   // cancelled sibling: ignored
        XCTAssertEqual(sub.values, [1, 2])
    }

    func testMergeManyFromArray() {
        let subjects = [PassthroughSubject<Int, Never>(),
                        PassthroughSubject<Int, Never>(),
                        PassthroughSubject<Int, Never>()]
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        Publishers.MergeMany(subjects).subscribe(sub)

        subjects[0].send(1)
        subjects[2].send(3)
        subjects[1].send(2)
        XCTAssertEqual(sub.values, [1, 3, 2])

        for s in subjects { s.send(completion: .finished) }
        XCTAssertEqual(sub.completions.count, 1)
    }
}
