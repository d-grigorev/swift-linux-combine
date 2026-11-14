extension Publishers {

    /// A publisher that replaces any errors in the stream with a provided element.
    ///
    /// Official API: `struct ReplaceError<Upstream>` (with `Failure == Never`).
    ///
    /// Semantics (verified by the parity suite): the replacement element is
    /// demand-gated — delivered immediately when downstream demand exists,
    /// otherwise held until the next request.
    public struct ReplaceError<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Never

        /// The element with which to replace errors from the upstream publisher.
        public let output: Upstream.Output

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        public init(upstream: Upstream, output: Output) {
            self.upstream = upstream
            self.output = output
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, output: output))
        }
    }
}

extension Publishers.ReplaceError {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Upstream.Output == Downstream.Input {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let output: Upstream.Output
        private let downstream: Downstream
        private var status = SubscriptionStatus.awaitingSubscription
        private var terminated = false
        private var pendingDemand = Subscribers.Demand.none
        private let lock = Lock()

        fileprivate init(downstream: Downstream, output: Upstream.Output) {
            self.downstream = downstream
            self.output = output
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
            guard case .subscribed = status else {
                lock.unlock()
                return .none
            }
            pendingDemand -= 1
            lock.unlock()
            let demand = downstream.receive(input)
            guard demand != .none else { return .none }
            lock.lock()
            pendingDemand += demand
            lock.unlock()
            return demand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = status, !terminated else {
                lock.unlock()
                return
            }
            switch completion {
            case .finished:
                status = .terminal
                lock.unlock()
                downstream.receive(completion: .finished)
            case .failure:
                // Without downstream demand the replacement value waits for
                // the next request instead of being delivered eagerly.
                if pendingDemand == .none {
                    terminated = true
                    lock.unlock()
                    return
                }
                status = .terminal
                lock.unlock()
                _ = downstream.receive(output)
                downstream.receive(completion: .finished)
            }
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            if terminated {
                status = .terminal
                lock.unlock()
                _ = downstream.receive(output)
                downstream.receive(completion: .finished)
                return
            }
            pendingDemand += demand
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

        var description: String { "ReplaceError" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Replaces any error in the stream with the provided element.
    public func replaceError(with output: Output) -> Publishers.ReplaceError<Self> {
        .init(upstream: self, output: output)
    }
}
