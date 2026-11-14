import XCTest
@testable import LinuxCombine

private struct RecordFailure: Error, Codable, Equatable {}

final class RecordTests: XCTestCase {

    func testEmptyRecordCompletesImmediatelyWithoutDemand() {
        let record = Record<Int, Never>(output: [], completion: .finished)
        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        record.subscribe(sub)

        XCTAssertEqual(sub.subscriptionCount, 1)
        XCTAssertEqual(sub.completions.count, 1, "empty record is not demand-gated")
        XCTAssertTrue(sub.values.isEmpty)
    }

    func testReplayIsDemandGated() {
        let record = Record<Int, Never>(output: [1, 2, 3], completion: .finished)
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .max(2)
        record.subscribe(sub)

        XCTAssertEqual(sub.values, [1, 2])
        XCTAssertTrue(sub.completions.isEmpty, "completion waits behind the last element")

        sub.firstSubscription?.request(.max(1))
        XCTAssertEqual(sub.values, [1, 2, 3])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testUnlimitedDemandReplaysEverythingAtOnce() {
        let record = Record<Int, Never>(output: [1, 2], completion: .finished)
        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        record.subscribe(sub)
        XCTAssertEqual(sub.values, [1, 2])
        XCTAssertEqual(sub.completions.count, 1)
    }

    func testRecordClosureBuildsRecording() {
        let record = Record<Int, Never> { recording in
            recording.receive(10)
            recording.receive(20)
        }
        XCTAssertEqual(record.recording.output, [10, 20])
        XCTAssertEqual(record.recording.completion, .finished)

        let sub = TestSubscriber<Int, Never>()
        sub.initialDemand = .unlimited
        record.subscribe(sub)
        XCTAssertEqual(sub.values, [10, 20])
    }

    func testFailureCompletionReplays() {
        let record = Record<Int, RecordFailure>(
            output: [1],
            completion: .failure(RecordFailure())
        )
        let sub = TestSubscriber<Int, RecordFailure>()
        sub.initialDemand = .max(1)
        record.subscribe(sub)

        XCTAssertEqual(sub.values, [1])
        XCTAssertEqual(sub.completions.count, 1)
        guard case .failure = sub.completions[0] else {
            return XCTFail("expected failure completion")
        }
    }

    func testRecordingInitialState() {
        var recording = Record<Int, Never>.Recording()
        XCTAssertEqual(recording.output, [])
        XCTAssertEqual(recording.completion, .finished)

        recording.receive(1)
        recording.receive(completion: .finished)
        XCTAssertEqual(recording.output, [1])

        let fromRecording = Record(recording: recording)
        XCTAssertEqual(fromRecording.recording.output, [1])
    }

    func testRecordingCodableRoundtrip() throws {
        let recording = Record<Int, RecordFailure>.Recording(
            output: [1, 2, 3],
            completion: .finished
        )
        let data = try JSONEncoder().encode(recording)
        let back = try JSONDecoder().decode(
            Record<Int, RecordFailure>.Recording.self,
            from: data
        )
        XCTAssertEqual(back.output, recording.output)
        XCTAssertEqual(back.completion, recording.completion)
    }

    func testRecordCodableRoundtrip() throws {
        let record = Record<String, RecordFailure>(
            output: ["a", "b"],
            completion: .failure(RecordFailure())
        )
        let data = try JSONEncoder().encode(record)
        let back = try JSONDecoder().decode(
            Record<String, RecordFailure>.self,
            from: data
        )
        XCTAssertEqual(back.recording.output, ["a", "b"])
        XCTAssertEqual(back.recording.completion, .failure(RecordFailure()))
    }

    func testCancelStopsReplay() {
        let record = Record<Int, Never>(output: [1, 2, 3], completion: .finished)
        let sub = TestSubscriber<Int, Never>()
        sub.autoRequest = false
        record.subscribe(sub)
        sub.firstSubscription?.cancel()
        sub.firstSubscription?.request(.max(3))

        XCTAssertTrue(sub.values.isEmpty)
        XCTAssertTrue(sub.completions.isEmpty)
    }

    func testMultipleSubscribersReplayIndependently() {
        let record = Record<Int, Never>(output: [1], completion: .finished)
        let a = TestSubscriber<Int, Never>()
        let b = TestSubscriber<Int, Never>()
        a.initialDemand = .unlimited
        b.initialDemand = .unlimited
        record.subscribe(a)
        record.subscribe(b)
        XCTAssertEqual(a.values, [1])
        XCTAssertEqual(b.values, [1])
        XCTAssertEqual(a.completions.count, 1)
        XCTAssertEqual(b.completions.count, 1)
    }
}
