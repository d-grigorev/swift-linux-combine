extension Publishers {

    /// A publisher that replaces an empty stream with a provided element.
    ///
    /// Official API: `struct ReplaceEmpty<Upstream>` with
    /// `init(upstream:output:)` and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): the upstream is requested
    /// `.unlimited`; the replacement element is delivered only if the
    /// upstream finished *without* delivering any elements AND the
    /// downstream has made at least one request (otherwise the
    /// element is held until the first request).
    public struct ReplaceEmpty<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The element to deliver when the upstream publisher finishes
        /// without delivering any elements.
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

extension Publisher {
    /// Replaces an empty stream with the provided element.
    public func replaceEmpty(with output: Output) -> Publishers.ReplaceEmpty<Self> {
        .init(upstream: self, output: output)
    }
}

extension Publishers.ReplaceEmpty: Equatable
where Upstream: Equatable, Upstream.Output: Equatable {}

extension Publishers.ReplaceEmpty {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let output: Output
        private let downstream: Downstream
        private var receivedUpstream = false
        private var downstreamRequested = false
        private var finishedWithoutUpstream = false
        private var status = SubscriptionStatus.awaitingSubscription

        fileprivate init(downstream: Downstream, output: Output) {
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
            subscription.request(.unlimited)
        }

        func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return .none
            }
            receivedUpstream = true
            lock.unlock()
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            status = .terminal
            if receivedUpstream {
                lock.unlock()
                downstream.receive(completion: completion)
                return
            }
            switch completion {
            case .finished:
                if downstreamRequested {
                    lock.unlock()
                    _ = downstream.receive(output)
                    downstream.receive(completion: completion)
                    return
                }
                finishedWithoutUpstream = true
                lock.unlock()
            case .failure:
                lock.unlock()
                downstream.receive(completion: completion)
            }
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            downstreamRequested = true
            if finishedWithoutUpstream {
                lock.unlock()
                _ = downstream.receive(output)
                downstream.receive(completion: .finished)
                return
            }
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

        var description: String { "ReplaceEmpty" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }
    }
}
