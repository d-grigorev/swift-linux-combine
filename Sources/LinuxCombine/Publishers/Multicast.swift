extension Publishers {

    /// A publisher that uses a subject to deliver elements to multiple
    /// subscribers.
    ///
    /// Official API: `final class Multicast<Upstream, SubjectType>` with
    /// `init(upstream:createSubject:)`, conforming to
    /// `ConnectablePublisher`.
    ///
    /// Semantics (verified by the parity suite): the subject is created
    /// lazily on first subscriber attachment (or `connect()`); each
    /// downstream gets an inner that forwards demands to the subject's
    /// subscription; `connect()` subscribes the subject to the upstream.
    public final class Multicast<Upstream: Publisher, SubjectType: Subject>:
        ConnectablePublisher
    where Upstream.Failure == SubjectType.Failure,
          Upstream.Output == SubjectType.Output {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher that this publisher receives elements from.
        public let upstream: Upstream

        /// A closure to create a new Subject each time a subscriber
        /// attaches to the multicast publisher.
        public let createSubject: () -> SubjectType

        private let lock = Lock()
        private var subject: SubjectType?

        private var lazySubject: SubjectType {
            lock.lock()
            if let subject {
                lock.unlock()
                return subject
            }
            let subject = createSubject()
            self.subject = subject
            lock.unlock()
            return subject
        }

        public init(upstream: Upstream, createSubject: @escaping () -> SubjectType) {
            self.upstream = upstream
            self.createSubject = createSubject
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            lazySubject.subscribe(Inner(parent: self, downstream: subscriber))
        }

        public func connect() -> any Cancellable {
            upstream.subscribe(lazySubject)
        }
    }
}

extension Publisher {
    /// Applies a closure to create a subject that delivers elements to
    /// subscribers.
    public func multicast<SubjectType: Subject>(
        _ createSubject: @escaping () -> SubjectType
    ) -> Publishers.Multicast<Self, SubjectType>
    where Failure == SubjectType.Failure, Output == SubjectType.Output {
        .init(upstream: self, createSubject: createSubject)
    }

    /// Provides a subject to deliver elements to multiple subscribers.
    public func multicast<SubjectType: Subject>(
        subject: SubjectType
    ) -> Publishers.Multicast<Self, SubjectType>
    where Failure == SubjectType.Failure, Output == SubjectType.Output {
        multicast { subject }
    }
}

extension Publishers.Multicast {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private enum State {
            case ready(downstream: Downstream)
            case subscribed(downstream: Downstream,
                            subjectSubscription: any Subscription)
            case terminal
        }

        private let lock = Lock()
        private var state: State

        fileprivate init(parent: Publishers.Multicast<Upstream, SubjectType>,
                         downstream: Downstream) {
            state = .ready(downstream: downstream)
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case let .ready(downstream) = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(downstream: downstream,
                                subjectSubscription: subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case let .subscribed(downstream, subjectSubscription) = state else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            let newDemand = downstream.receive(input)
            if newDemand > 0 {
                subjectSubscription.request(newDemand)
            }
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case let .subscribed(downstream, _) = state else {
                lock.unlock()
                return
            }
            // Terminal drops the state-embedded downstream afterwards
            // (lifecycle tests pin the release).
            state = .terminal
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(_, subjectSubscription) = state else {
                lock.unlock()
                return
            }
            lock.unlock()
            subjectSubscription.request(demand)
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(_, subjectSubscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            subjectSubscription.cancel()
        }

        var description: String { "Multicast" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }
    }
}
