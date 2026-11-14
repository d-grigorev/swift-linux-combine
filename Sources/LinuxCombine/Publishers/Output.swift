extension Publisher {

    /// Republishes elements up to the specified maximum count.
    ///
    /// Official API (verified against Apple DocC):
    /// `prefix(_ maxLength: Int) -> Publishers.Output<Self>`.
    public func prefix(_ maxLength: Int) -> Publishers.Output<Self> {
        output(in: ..<maxLength)
    }

    /// Publishes a specific element, indicated by its index in the
    /// sequence of published elements.
    ///
    /// Official API (verified against Apple DocC):
    /// `output(at index: Int) -> Publishers.Output<Self>`.
    public func output(at index: Int) -> Publishers.Output<Self> {
        output(in: index...index)
    }

    /// Publishes elements specified by their range in the sequence of
    /// published elements.
    ///
    /// Official API (verified against Apple DocC):
    /// `output<R>(in range: R) -> Publishers.Output<Self> where R:
    /// RangeExpression, R.Bound == Int`.
    public func output<R>(in range: R) -> Publishers.Output<Self>
    where R: RangeExpression, R.Bound == Int {
        .init(upstream: self, range: range.relative(to: 0 ..< .max))
    }
}

extension Publishers {

    /// A publisher that publishes elements specified by a range in the
    /// sequence of published elements.
    ///
    /// Official API: `struct Output<Upstream>` with
    /// `init(upstream:range:)` and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): skipped elements are
    /// re-requested one at a time (`.max(1)` per dropped element); when
    /// the range is exhausted the upstream is cancelled and the
    /// downstream receives `.finished`.
    public struct Output<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The range of elements to publish.
        public let range: CountableRange<Int>

        public init(upstream: Upstream, range: CountableRange<Int>) {
            precondition(range.lowerBound >= 0, "lowerBound must not be negative")
            precondition(range.upperBound >= 0, "upperBound must not be negative")
            self.upstream = upstream
            self.range = range
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, range: range))
        }
    }
}

extension Publishers.Output: Equatable where Upstream: Equatable {}

extension Publishers.Output {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private var status = SubscriptionStatus.awaitingSubscription
        private var remainingUntilStart: Int
        private var remainingCount: Int

        fileprivate init(downstream: Downstream, range: CountableRange<Int>) {
            self.downstream = downstream
            self.remainingUntilStart = range.lowerBound
            self.remainingCount = range.count
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = status else {
                lock.unlock()
                subscription.cancel()
                return
            }
            status = .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if remainingUntilStart > 0 {
                remainingUntilStart -= 1
                lock.unlock()
                return .max(1)
            }
            let newDemand: Subscribers.Demand
            if remainingCount > 0 {
                remainingCount -= 1
                lock.unlock()
                newDemand = downstream.receive(input)
            } else {
                lock.unlock()
                newDemand = .none
                cancelUpstreamAndFinish()
            }
            return newDemand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "Output" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }

        private func cancelUpstreamAndFinish() {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
            downstream.receive(completion: .finished)
        }
    }
}
