import XCTest
@testable import LinuxCombine

/// Own tests for `comparison(areInIncreasingOrder:)` /
/// `tryComparison(areInIncreasingOrder:)`.
///
/// These official methods have no DocC page in the current documentation
/// JSON (docs/QUIRKS.md §C C8 — 404, like `Optional.Publisher`), so the
/// borrowed `ComparisonTests` (which exercise `min`/`max` sugar) don't
/// cover them. They are pure sugar over the `Comparison`/`TryComparison`
/// inits; these tests pin that equivalence.
final class ComparisonSugarTests: XCTestCase {

    func testComparisonMatchesMaxBy() {
        let viaSugar = TestSubscriber<Int, Never>()
        viaSugar.initialDemand = .unlimited
        [3, 1, 4, 1, 5].publisher
            .comparison(areInIncreasingOrder: <)
            .subscribe(viaSugar)

        let viaMaxBy = TestSubscriber<Int, Never>()
        viaMaxBy.initialDemand = .unlimited
        [3, 1, 4, 1, 5].publisher.max(by: <).subscribe(viaMaxBy)

        XCTAssertEqual(viaSugar.values, viaMaxBy.values)
        XCTAssertEqual(viaSugar.values, [5])
        XCTAssertEqual(viaSugar.completions.count, 1)
    }

    func testTryComparisonFailure() {
        struct Boom: Error {}

        let sub = TestSubscriber<Int, Error>()
        sub.initialDemand = .unlimited
        [1, 2, 3].publisher.tryComparison { _, _ in throw Boom() }.subscribe(sub)

        XCTAssertEqual(sub.values, [])
        XCTAssertEqual(sub.completions.count, 1)
        if case .failure = sub.completions[0] {} else {
            XCTFail("expected failure")
        }
    }

    func testComparisonEmptyEmitsOnlyFinished() {
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        Empty<Int, Never>().comparison(areInIncreasingOrder: <).subscribe(sub)

        XCTAssertEqual(sub.values, [])
        XCTAssertEqual(sub.completions.count, 1)
        if case .finished = sub.completions[0] {} else {
            XCTFail("expected finished")
        }
    }
}
