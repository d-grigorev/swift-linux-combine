extension Publishers {

    /// A publisher that buffers a maximum number of items.
    ///
    /// Official API: `struct CollectByCount<Upstream>` with
    /// `init(upstream:count:)` and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): requests
    /// `.max(demand * count)` from the upstream for each downstream request;
    /// a completed buffer is emitted with the downstream's returned demand
    /// multiplied back by `count`; a partial buffer flushes on upstream
    /// completion; failures drop the buffer.
    public struct CollectByCount<Upstream: Publisher>: Publisher {
        public typealias Output = [Upstream.Output]
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let count: Int

        public init(upstream: Upstream, count: Int) {
            self.upstream = upstream
            self.count = count
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, count: count))
        }
    }
}

extension Publishers.CollectByCount: Equatable where Upstream: Equatable {}

extension Publisher {
    /// Collects up to the specified number of elements, then emits the array.
    ///
    /// Note: a downstream request of `.max(n)` becomes `.max(count * n)`
    /// towards the upstream.
    public func collect(_ count: Int) -> Publishers.CollectByCount<Self> {
        .init(upstream: self, count: count)
    }
}

extension Publishers.CollectByCount {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == [Upstream.Output], Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let count: Int
        private var buffer: [Input] = []
        private var subscription: (any Subscription)?
        private var finished = false
        private let lock = Lock()

        init(downstream: Downstream, count: Int) {
            self.downstream = downstream
            self.count = count
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            if finished || self.subscription != nil {
                lock.unlock()
                subscription.cancel()
                return
            }
            self.subscription = subscription
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if subscription == nil {
                lock.unlock()
                return .none
            }
            buffer.append(input)
            guard buffer.count == count else {
                lock.unlock()
                return .none
            }
            let output = buffer
            buffer = []
            lock.unlock()
            return downstream.receive(output) * count
        }

        func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            lock.lock()
            subscription = nil
            finished = true
            let partial: [Input]
            switch completion {
            case .finished:
                if buffer.isEmpty {
                    lock.unlock()
                    partial = []
                } else {
                    partial = buffer
                    buffer = []
                    lock.unlock()
                    _ = downstream.receive(partial)
                }
            case .failure:
                buffer = []
                lock.unlock()
                partial = []
            }
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            if let subscription {
                lock.unlock()
                subscription.request(demand * count)
            } else {
                lock.unlock()
            }
        }

        func cancel() {
            lock.lock()
            if let subscription {
                buffer = []
                finished = true
                self.subscription = nil
                lock.unlock()
                subscription.cancel()
            } else {
                lock.unlock()
            }
        }

        var description: String { "CollectByCount" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream),
                ("upstreamSubscription", subscription as Any),
                ("buffer", buffer),
                ("count", count),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
