extension Publishers {

    /// A publisher that omits a specified number of elements.
    ///
    /// Official API: `struct Drop<Upstream>` (backing `dropFirst(_:)`).
    ///
    /// Semantics (verified in full by the parity suite):
    /// - The downstream subscription is granted **eagerly** at
    ///   `receive(subscriber:)`, before the upstream subscription exists.
    /// - Requests made before the upstream subscription arrives are buffered;
    ///   on arrival (and on later requests) the upstream receives
    ///   `pendingDownstreamDemand + remainingDrops` (skip-count
    ///   pre-compensation), forwarding nothing when that computes to zero.
    /// - Skipped elements return `.none` (already pre-paid); passing elements
    ///   return the downstream's demand. Neither is status-gated.
    /// - Completions are forwarded blindly, but detach the upstream without
    ///   cancelling; `cancel()` cancels only a still-attached upstream.
    /// - A second upstream subscription is cancelled while one is attached.
    public struct Drop<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let count: Int

        public init(upstream: Upstream, count: Int) {
            self.upstream = upstream
            self.count = count
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber, count: Swift.max(0, count))
            // Eager grant: the downstream gets its subscription immediately.
            subscriber.receive(subscription: inner)
            upstream.subscribe(inner)
        }
    }
}

extension Publishers.Drop {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private var remaining: Int
        private var pendingDemand = Subscribers.Demand.none
        private var upstream: (any Subscription)?
        private let lock = Lock()

        fileprivate init(downstream: Downstream, count: Int) {
            self.downstream = downstream
            self.remaining = count
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            if upstream != nil {
                // An upstream is already attached: reject the new one.
                lock.unlock()
                subscription.cancel()
                return
            }
            upstream = subscription
            let flush = pendingDemand + Subscribers.Demand.max(remaining)
            pendingDemand = .none
            lock.unlock()
            if flush > 0 {
                subscription.request(flush)
            }
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if remaining > 0 {
                remaining -= 1
                lock.unlock()
                return .none   // skips are pre-paid via the request formula
            }
            lock.unlock()
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            // Completions are forwarded blindly (even repeatedly, even after
            // cancellation) but detach the upstream without cancelling it.
            lock.lock()
            upstream = nil
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            pendingDemand += demand
            guard let upstream else {
                lock.unlock()
                return
            }
            let flush = pendingDemand + Subscribers.Demand.max(remaining)
            pendingDemand = .none
            lock.unlock()
            if flush > 0 {
                upstream.request(flush)
            }
        }

        func cancel() {
            lock.lock()
            let upstream = self.upstream
            self.upstream = nil
            lock.unlock()
            upstream?.cancel()
        }

        var description: String { "Drop" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Omits the specified number of leading elements.
    public func dropFirst(_ count: Int = 1) -> Publishers.Drop<Self> {
        .init(upstream: self, count: count)
    }
}
