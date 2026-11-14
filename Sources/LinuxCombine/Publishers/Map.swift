extension Publishers {

    /// A publisher that transforms all elements from the upstream publisher
    /// with a provided closure.
    ///
    /// Official API verified against Apple DocC: `struct Map<Upstream, Output>`.
    public struct Map<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The closure that transforms elements from the upstream publisher.
        public let transform: (Upstream.Output) -> Output

        public init(upstream: Upstream, transform: @escaping (Upstream.Output) -> Output) {
            self.upstream = upstream
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, map: transform))
        }
    }

    /// A publisher that transforms all elements from the upstream publisher
    /// with an error-throwing closure.
    ///
    /// Official API verified against Apple DocC: `struct TryMap<Upstream, Output>`
    /// (with `Failure == Error`).
    public struct TryMap<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Error

        public let upstream: Upstream
        public let transform: (Upstream.Output) throws -> Output

        public init(upstream: Upstream, transform: @escaping (Upstream.Output) throws -> Output) {
            self.upstream = upstream
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, map: transform))
        }
    }
}

extension Publishers.Map {

    /// Stateless relay: the upstream subscription is passed straight through
    /// (the downstream's subscription *is* the upstream's).
    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let map: (Input) -> Output
        let combineIdentifier = CombineIdentifier()

        fileprivate init(downstream: Downstream, map: @escaping (Input) -> Output) {
            self.downstream = downstream
            self.map = map
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            return downstream.receive(map(input))
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "Map" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryMap {

    /// Stateful relay: may cancel the upstream when the transform throws.
    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let map: (Input) throws -> Output
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(downstream: Downstream, map: @escaping (Input) throws -> Output) {
            self.downstream = downstream
            self.map = map
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
            do {
                return try downstream.receive(map(input))
            } catch {
                lock.lock()
                let subscription = status.subscription
                status = .terminal
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let error):
                downstream.receive(completion: .failure(error as any Error))
            }
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

        var description: String { "TryMap" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Transforms all elements from the upstream publisher with a provided closure.
    public func map<T>(_ transform: @escaping (Output) -> T) -> Publishers.Map<Self, T> {
        .init(upstream: self, transform: transform)
    }

    /// Transforms all elements with an error-throwing closure, terminating
    /// with the thrown error.
    public func tryMap<T>(_ transform: @escaping (Output) throws -> T)
    -> Publishers.TryMap<Self, T> {
        .init(upstream: self, transform: transform)
    }

    /// Replaces `nil` elements with the given element.
    public func replaceNil<T>(with output: T) -> Publishers.Map<Self, T>
    where Output == T? {
        .init(upstream: self) { $0 ?? output }
    }
}

extension Publishers.Map {

    /// Operator specialization: composes the transforms over one upstream
    /// (verified by the parity suite — `upstream` identity is preserved).
    public func map<T>(_ transform: @escaping (Output) -> T)
    -> Publishers.Map<Upstream, T> {
        .init(upstream: upstream) { transform(self.transform($0)) }
    }

    public func tryMap<T>(_ transform: @escaping (Output) throws -> T)
    -> Publishers.TryMap<Upstream, T> {
        .init(upstream: upstream) { try transform(self.transform($0)) }
    }
}

extension Publishers.TryMap {

    /// Operator specialization: stays a `TryMap`, composing transforms.
    public func map<T>(_ transform: @escaping (Output) -> T)
    -> Publishers.TryMap<Upstream, T> {
        .init(upstream: upstream) { try transform(self.transform($0)) }
    }

    public func tryMap<T>(_ transform: @escaping (Output) throws -> T)
    -> Publishers.TryMap<Upstream, T> {
        .init(upstream: upstream) { try transform(self.transform($0)) }
    }
}
