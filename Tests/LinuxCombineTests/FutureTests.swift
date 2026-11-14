import XCTest
@testable import LinuxCombine

private struct FutureError: Error, Equatable {}

final class FutureTests: XCTestCase {

    func testAttemptClosureRunsEagerlyAtInit() {
        var runs = 0
        _ = Future<Int, Never> { promise in
            runs += 1
            promise(.success(5))
        }
        XCTAssertEqual(runs, 1, "Future runs its attempt closure eagerly, once")
    }

    func testOnlyFirstPromiseWins() {
        var promises: [Future<Int, Never>.Promise] = []
        let future = Future<Int, Never> { promises.append($0) }
        XCTAssertEqual(promises.count, 1)

        promises[0](.success(1))
        promises[0](.success(2))   // ignored

        let sub = TestSubscriber<Int, Never>()
        future.subscribe(sub)
        XCTAssertEqual(sub.values, [1])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testSuccessReplaysToEverySubscriberAfterResolution() {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }
        promise(.success(9))

        let a = TestSubscriber<Int, Never>()
        let b = TestSubscriber<Int, Never>()
        future.subscribe(a)
        future.subscribe(b)
        XCTAssertEqual(a.values, [9])
        XCTAssertEqual(b.values, [9])
        XCTAssertEqual(a.completions.count, 1)
        XCTAssertEqual(b.completions.count, 1)
    }

    func testSuccessWaitsForDemand() {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }
        promise(.success(9))

        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        future.subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertTrue(sub.values.isEmpty, "success is demand-gated")

        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [9])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testFailureDeliveredWithoutAnyRequest() {
        var promise: Future<Int, FutureError>.Promise!
        let future = Future<Int, FutureError> { promise = $0 }
        promise(.failure(FutureError()))

        let sub = TestSubscriber<Int, FutureError>()
        sub.autoRequest = false
        future.subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.completions.count, 1, "failure is not demand-gated")
        guard case .failure = sub.completions[0] else {
            return XCTFail("expected failure")
        }
    }

    func testSubscriptionBeforeResolution() {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }

        let sub = TestSubscriber<Int, Never>()
        future.subscribe(sub)
        XCTAssertTrue(sub.values.isEmpty)

        promise(.success(4))   // arrives after the subscriber requested
        XCTAssertEqual(sub.values, [4])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testCancelPreventsLaterDelivery() {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }

        let sub = TestSubscriber<Int, Never>()
        future.subscribe(sub)
        sub.firstSubscription?.cancel()

        promise(.success(4))
        XCTAssertTrue(sub.values.isEmpty)
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testSecondSubscriberBeforeResolutionAlsoGetsValue() {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }

        let a = TestSubscriber<Int, Never>()
        let b = TestSubscriber<Int, Never>()
        future.subscribe(a)
        future.subscribe(b)

        promise(.success(3))
        XCTAssertEqual(a.values, [3])
        XCTAssertEqual(b.values, [3])
    }

    // MARK: - async value access

    func testAsyncValueSuccessNeverFailure() async {
        let future = Future<Int, Never> { $0(.success(41)) }
        let value = await future.value
        XCTAssertEqual(value, 41)
    }

    func testAsyncValueWaitsForLateResolution() async {
        var promise: Future<Int, Never>.Promise!
        let future = Future<Int, Never> { promise = $0 }

        async let pending = future.value
        // Give the async getter time to subscribe, then resolve.
        await Task.yield()
        promise(.success(7))
        let value = await pending
        XCTAssertEqual(value, 7)
    }

    func testAsyncValueThrowsOnFailure() async {
        let future = Future<Int, FutureError> { $0(.failure(FutureError())) }
        do {
            _ = try await future.value
            XCTFail("expected throw")
        } catch {
            XCTAssertEqual(error as? FutureError, FutureError())
        }
    }

    func testAsyncValueSucceedsForThrowingFuture() async throws {
        let future = Future<Int, FutureError> { $0(.success(6)) }
        let value = try await future.value
        XCTAssertEqual(value, 6)
    }
}
