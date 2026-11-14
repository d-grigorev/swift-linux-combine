import LinuxCombine
import Foundation

// MARK: - Test doubles shared by the suites

/// A subscription that records requests and cancellation.
final class TestSubscription: Subscription {
    let combineIdentifier = CombineIdentifier()
    private(set) var requested: [Subscribers.Demand] = []
    private(set) var cancelled = false

    /// Continuation hook: called for every `request(_:)`, letting a test drive
    /// synchronous upstream delivery.
    var onRequest: ((Subscribers.Demand) -> Void)?
    var onCancel: (() -> Void)?

    func request(_ demand: Subscribers.Demand) {
        requested.append(demand)
        onRequest?(demand)
    }

    func cancel() {
        cancelled = true
        onCancel?()
    }
}

/// A subscriber that records every event in order.
final class TestSubscriber<Input, Failure: Error>: Subscriber {
    enum Event {
        case subscription(any Subscription)
        case value(Input)
        case completion(Subscribers.Completion<Failure>)
    }

    private(set) var events: [Event] = []

    /// Demand requested immediately upon receiving the subscription.
    var initialDemand: Subscribers.Demand = .max(1)

    /// Whether `initialDemand` is requested automatically on subscription.
    var autoRequest = true

    /// Invoked for every value delivered to the subscriber (after it is
    /// recorded).
    var onValue: ((Input) -> Void)?
    /// Extra demand returned from each `receive(_:)`.
    var additionalDemandPerValue: Subscribers.Demand = .none

    let combineIdentifier = CombineIdentifier()

    func receive(subscription: any Subscription) {
        events.append(.subscription(subscription))
        if autoRequest {
            subscription.request(initialDemand)
        }
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        events.append(.value(input))
        onValue?(input)
        return additionalDemandPerValue
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        events.append(.completion(completion))
    }

    var subscriptionCount: Int {
        events.reduce(into: 0) { count, event in
            if case .subscription = event { count += 1 }
        }
    }

    var values: [Input] {
        var out: [Input] = []
        for event in events {
            if case .value(let v) = event { out.append(v) }
        }
        return out
    }

    var completions: [Subscribers.Completion<Failure>] {
        var out: [Subscribers.Completion<Failure>] = []
        for event in events {
            if case .completion(let c) = event { out.append(c) }
        }
        return out
    }

    var firstSubscription: (any Subscription)? {
        for event in events {
            if case .subscription(let s) = event { return s }
        }
        return nil
    }
}

/// A minimal publisher whose behavior is the provided closure.
struct TestPublisher<Output, Failure: Error>: Publisher {
    private let body: (AnySubscriber<Output, Failure>) -> Void

    init(_ body: @escaping (AnySubscriber<Output, Failure>) -> Void) {
        self.body = body
    }

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        body(AnySubscriber(subscriber))
    }
}

/// A subject that only records `send` calls; the publisher side is unused.
final class TestSubject: Subject {
    typealias Output = Int
    typealias Failure = Never

    enum Send {
        case subscription(any Subscription)
        case value(Int)
        case completion(Subscribers.Completion<Never>)
    }

    private(set) var sent: [Send] = []

    var lastSubscription: (any Subscription)? {
        for send in sent {
            if case .subscription(let s) = send { return s }
        }
        return nil
    }

    var values: [Int] {
        var out: [Int] = []
        for send in sent {
            if case .value(let v) = send { out.append(v) }
        }
        return out
    }

    func send(_ value: Int) {
        sent.append(.value(value))
    }

    func send(completion: Subscribers.Completion<Never>) {
        sent.append(.completion(completion))
    }

    func send(subscription: any Subscription) {
        sent.append(.subscription(subscription))
    }

    func receive<S: Subscriber>(subscriber: S)
    where S.Input == Int, S.Failure == Never {
        // Not exercised: TestSubject is only attached as a subject in these tests.
    }
}
