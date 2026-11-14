import XCTest
@testable import LinuxCombine

final class ErasureTests: XCTestCase {

    // MARK: - AnyCancellable

    func testCancelRunsClosureOnce() {
        var runs = 0
        let cancellable = AnyCancellable { runs += 1 }
        cancellable.cancel()
        cancellable.cancel()
        XCTAssertEqual(runs, 1)
    }

    func testDeinitCancelsAutomatically() {
        var runs = 0
        var cancellable: AnyCancellable? = AnyCancellable { runs += 1 }
        XCTAssertEqual(runs, 0)
        XCTAssertNotNil(cancellable)
        cancellable = nil
        XCTAssertEqual(runs, 1)
    }

    func testDeinitAfterCancelDoesNotReRun() {
        var runs = 0
        var cancellable: AnyCancellable? = AnyCancellable { runs += 1 }
        cancellable?.cancel()
        cancellable = nil
        XCTAssertEqual(runs, 1)
    }

    func testWrappingAnotherCancellable() {
        let mock = TestSubscription()
        let cancellable = AnyCancellable(mock)
        cancellable.cancel()
        cancellable.cancel()
        XCTAssertTrue(mock.cancelled)
    }

    func testEqualityAndHashingAreIdentityBased() {
        let a = AnyCancellable {}
        let b = AnyCancellable {}
        XCTAssertEqual(a, a)
        XCTAssertNotEqual(a, b)

        var copy: AnyCancellable? = a
        XCTAssertEqual(copy, a)
        XCTAssertNotEqual(copy, b)
        copy = nil

        var set = Set<AnyCancellable>()
        set.insert(a)
        set.insert(a)
        set.insert(b)
        XCTAssertEqual(set.count, 2)
        XCTAssertTrue(set.contains(a))
        XCTAssertTrue(set.contains(b))
    }

    func testStoreInSetInsertsSelf() {
        let cancellable = AnyCancellable {}
        var set = Set<AnyCancellable>()
        cancellable.store(in: &set)
        XCTAssertEqual(set.count, 1)
        XCTAssertTrue(set.contains(cancellable))  // identity, not a wrapper
    }

    func testStoreInArrayAppendsSelf() {
        let cancellable = AnyCancellable {}
        var array: [AnyCancellable] = []
        cancellable.store(in: &array)
        XCTAssertEqual(array.count, 1)
        XCTAssertTrue(array[0] === cancellable)
    }

    func testProtocolExtensionStoreWrapsPlainCancellable() {
        let mock = TestSubscription()
        var set = Set<AnyCancellable>()
        mock.store(in: &set)
        XCTAssertEqual(set.count, 1)
        set.first?.cancel()
        XCTAssertTrue(mock.cancelled)

        var array: [AnyCancellable] = []
        let mock2 = TestSubscription()
        mock2.store(in: &array)
        XCTAssertEqual(array.count, 1)
        array[0].cancel()
        XCTAssertTrue(mock2.cancelled)
    }

    // MARK: - AnySubscriber

    func testClosureBasedInitForwardsEverything() {
        var receivedSubscription: (any Subscription)?
        var receivedValues: [Int] = []
        var receivedCompletions: [Subscribers.Completion<Never>] = []

        let subscriber = AnySubscriber<Int, Never>(
            receiveSubscription: { receivedSubscription = $0 },
            receiveValue: { value in
                receivedValues.append(value)
                return .max(2)
            },
            receiveCompletion: { receivedCompletions.append($0) }
        )

        let subscription = TestSubscription()
        subscriber.receive(subscription: subscription)
        XCTAssertTrue(receivedSubscription as? TestSubscription === subscription)

        XCTAssertEqual(subscriber.receive(7), .max(2))
        XCTAssertEqual(receivedValues, [7])

        subscriber.receive(completion: .finished)
        XCTAssertEqual(receivedCompletions.count, 1)
        if case .finished = receivedCompletions[0] {} else {
            XCTFail("expected .finished")
        }
    }

    func testClosureBasedInitDefaultsToNoopAndNone() {
        let subscriber = AnySubscriber<Int, Never>()
        subscriber.receive(subscription: TestSubscription())
        XCTAssertEqual(subscriber.receive(1), .none)
        subscriber.receive(completion: .finished)  // must not trap
    }

    func testClosureBasedInitHasFreshStableIdentifier() {
        let first = AnySubscriber<Int, Never>()
        let second = AnySubscriber<Int, Never>()
        XCTAssertNotEqual(first.combineIdentifier, second.combineIdentifier)

        let copy = first
        XCTAssertEqual(copy.combineIdentifier, first.combineIdentifier)
    }

    func testWrappingSubscriberPreservesIdentifierAndForwards() {
        let recorder = TestSubscriber<Int, Never>()
        let erased = AnySubscriber(recorder)

        XCTAssertEqual(erased.combineIdentifier, recorder.combineIdentifier)

        let subscription = TestSubscription()
        recorder.initialDemand = .max(3)
        erased.receive(subscription: subscription)
        XCTAssertEqual(recorder.subscriptionCount, 1)
        XCTAssertEqual(subscription.requested, [.max(3)])

        _ = erased.receive(42)
        XCTAssertEqual(recorder.values, [42])

        erased.receive(completion: .finished)
        XCTAssertEqual(recorder.completions.count, 1)
    }

    func testDoubleWrappingUnwraps() {
        let recorder = TestSubscriber<Int, Never>()
        let once = AnySubscriber(recorder)
        let twice = AnySubscriber(once)
        XCTAssertEqual(twice.combineIdentifier, recorder.combineIdentifier)
        XCTAssertTrue(twice.description == once.description)
    }

    func testWrappingSubjectRelaysViaSend() {
        let subject = TestSubject()
        let erased = AnySubscriber(subject)
        let subscription = TestSubscription()

        erased.receive(subscription: subscription)
        // The relay sends *itself* as the subscription into the subject.
        guard let relay = subject.lastSubscription else {
            return XCTFail("subject should receive a subscription")
        }
        relay.request(.max(5))
        XCTAssertEqual(subscription.requested, [.max(5)])

        XCTAssertEqual(erased.receive(99), .none)
        XCTAssertEqual(subject.values, [99])

        erased.receive(completion: .finished)
        guard case .completion(.finished)? = subject.sent.last else {
            return XCTFail("subject should receive completion")
        }
        XCTAssertEqual(relay.combineIdentifier, subject.lastSubscription?.combineIdentifier)
    }

    // MARK: - AnyPublisher

    func testAnyPublisherForwardsSubscription() {
        var upstream: [AnySubscriber<Int, Never>] = []
        let publisher = TestPublisher<Int, Never> { upstream.append($0) }
        let erased = publisher.eraseToAnyPublisher()

        let recorder = TestSubscriber<Int, Never>()
        recorder.initialDemand = .max(4)
        erased.subscribe(recorder)

        XCTAssertEqual(upstream.count, 1)
        let subscription = TestSubscription()
        upstream[0].receive(subscription: subscription)
        XCTAssertEqual(subscription.requested, [.max(4)])

        _ = upstream[0].receive(1)
        upstream[0].receive(completion: .finished)
        XCTAssertEqual(recorder.values, [1])
        XCTAssertEqual(recorder.completions.count, 1)
    }

    func testAnyPublisherDescriptionMirrorsWrapped() {
        struct Named: Publisher {
            typealias Output = Int
            typealias Failure = Never
            func receive<S: Subscriber>(subscriber: S)
            where S.Input == Int, S.Failure == Never {}
        }
        // Official behavior (verified by the parity suite): the description
        // of AnyPublisher is the fixed string "AnyPublisher", never the
        // wrapped publisher's own description.
        let erased = AnyPublisher(Named())
        XCTAssertEqual(erased.description, "AnyPublisher")
        XCTAssertEqual(erased.playgroundDescription as? String, "AnyPublisher")
    }

    // MARK: - subscribe(subject)

    func testSubscribeSubjectReturnsCancellableAndRelays() {
        var upstream: [AnySubscriber<Int, Never>] = []
        let publisher = TestPublisher<Int, Never> { upstream.append($0) }
        let subject = TestSubject()
        let upstreamSubscription = TestSubscription()

        let cancellable = publisher.subscribe(subject)
        XCTAssertEqual(upstream.count, 1)

        // Upstream delivers its subscription; the relay enters the subject.
        upstream[0].receive(subscription: upstreamSubscription)
        guard let relay = subject.lastSubscription else {
            return XCTFail("subject should have received the relay subscription")
        }

        // Demand flows: subject subscriber -> relay -> upstream.
        relay.request(.max(3))
        XCTAssertEqual(upstreamSubscription.requested, [.max(3)])

        // Values flow: upstream -> relay -> subject.send.
        _ = upstream[0].receive(11)
        _ = upstream[0].receive(22)
        XCTAssertEqual(subject.values, [11, 22])

        // Completion flows.
        upstream[0].receive(completion: .finished)
        guard case .completion(.finished)? = subject.sent.last else {
            return XCTFail("subject should have received completion")
        }

        // Cancelling the returned cancellable tears down upstream.
        var upstream2: [AnySubscriber<Int, Never>] = []
        let publisher2 = TestPublisher<Int, Never> { upstream2.append($0) }
        let subject2 = TestSubject()
        let subscription2 = TestSubscription()
        let cancellable2 = publisher2.subscribe(subject2)
        upstream2[0].receive(subscription: subscription2)
        cancellable2.cancel()
        XCTAssertTrue(subscription2.cancelled)
        withExtendedLifetime(cancellable) {}
    }

    // MARK: - Subscriptions.empty

    func testEmptySubscriptionIgnoresRequestsAndCancellation() {
        let subscription = Subscriptions.empty
        subscription.request(.max(5))
        subscription.request(.unlimited)
        subscription.cancel()
        subscription.cancel()
        // Stable identity across accesses and usable as an existential.
        let again: any Subscription = Subscriptions.empty
        XCTAssertEqual(subscription.combineIdentifier, again.combineIdentifier)
    }

    // MARK: - Void sugar

    func testVoidSugar() {
        // Subscriber.receive() for Input == Void
        var values: [Void] = []
        let subscriber = AnySubscriber<Void, Never>(
            receiveValue: { values.append($0); return .none }
        )
        let demand = subscriber.receive()
        XCTAssertEqual(demand, .none)
        XCTAssertEqual(values.count, 1)

        // Subject.send() for Output == Void
        let subject = TestVoidSubject()
        subject.send()
        XCTAssertEqual(subject.sentCount, 1)
    }
}

/// Minimal Void-output subject for the sugar check.
private final class TestVoidSubject: Subject {
    typealias Output = Void
    typealias Failure = Never
    private(set) var sentCount = 0

    func send(_ value: Void) { sentCount += 1 }
    func send(completion: Subscribers.Completion<Never>) {}
    func send(subscription: any Subscription) {}
    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Void, S.Failure == Never {}
}
