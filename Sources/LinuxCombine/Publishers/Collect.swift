extension Publishers {

    /// A publisher that publishes an array containing all of the upstream
    /// publisher's elements after it finishes.
    ///
    /// Official API: `struct Collect<Upstream>`.
    ///
    /// Semantics (verified by the parity suite): pulls `.unlimited` from the
    /// upstream immediately, buffers every element (each `receive` returns
    /// `.none`), and emits the array plus `.finished` once the downstream has
    /// outstanding demand — even if the upstream finished earlier. An empty
    /// upstream emits an empty array. Failures are delivered immediately.
    public struct Collect<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = [Upstream.Output]

        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber))
        }
    }
}

extension Publishers.Collect {

    /// Mirror child (official): exactly `count`.
    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == [Upstream.Output], Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private var buffered: [Input] = []
        private var status = SubscriptionStatus.awaitingSubscription
        private var demand = Subscribers.Demand.none
        private var finishedPending = false
        private let lock = Lock()

        fileprivate init(downstream: Downstream) {
            self.downstream = downstream
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
            // Grant the downstream subscription FIRST (its callback may
            // observe upstream history), THEN pull .unlimited — official
            // order verified by the parity suite.
            downstream.receive(subscription: self)
            subscription.request(.unlimited)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return .none
            }
            buffered.append(input)
            lock.unlock()
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return
            }
            switch completion {
            case .failure(let error):
                status = .terminal
                lock.unlock()
                downstream.receive(completion: .failure(error))
            case .finished:
                if demand > 0 {
                    let values = buffered
                    status = .terminal
                    lock.unlock()
                    _ = downstream.receive(values)
                    downstream.receive(completion: .finished)
                } else {
                    finishedPending = true
                    lock.unlock()
                }
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            self.demand += demand
            guard finishedPending, self.demand > 0 else {
                lock.unlock()
                return
            }
            let values = buffered
            status = .terminal
            lock.unlock()
            _ = downstream.receive(values)
            downstream.receive(completion: .finished)
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

        var description: String { "Collect" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [("count", buffered.count)]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Collects all elements into a single array published after completion.
    public func collect() -> Publishers.Collect<Self> {
        .init(upstream: self)
    }
}
