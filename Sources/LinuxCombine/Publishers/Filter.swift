extension Publishers {

    /// A publisher that republishes elements that pass a test.
    ///
    /// Official API: `struct Filter<Upstream>`.
    public struct Filter<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let isIncluded: (Upstream.Output) -> Bool

        public init(upstream: Upstream, isIncluded: @escaping (Upstream.Output) -> Bool) {
            self.upstream = upstream
            self.isIncluded = isIncluded
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: isIncluded))
        }
    }

    /// A publisher that republishes elements that pass an error-throwing test.
    ///
    /// Official API: `struct TryFilter<Upstream>`.
    public struct TryFilter<Upstream: Publisher>: Publisher {
        public typealias Failure = Error
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let isIncluded: (Upstream.Output) throws -> Bool

        public init(upstream: Upstream, isIncluded: @escaping (Upstream.Output) throws -> Bool) {
            self.upstream = upstream
            self.isIncluded = isIncluded
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: isIncluded))
        }
    }
}

extension Publishers.Filter {

    /// Stateless relay; rejected elements replenish one upstream demand so
    /// the downstream's outstanding request keeps flowing.
    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input) -> Bool
        let combineIdentifier = CombineIdentifier()

        fileprivate init(downstream: Downstream, predicate: @escaping (Input) -> Bool) {
            self.downstream = downstream
            self.predicate = predicate
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            guard predicate(input) else { return .max(1) }
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "Filter" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryFilter {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input) throws -> Bool
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(downstream: Downstream, predicate: @escaping (Input) throws -> Bool) {
            self.downstream = downstream
            self.predicate = predicate
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
            if case .terminal = status {
                lock.unlock()
                return .none   // terminal: values are dropped (official)
            }
            lock.unlock()

            let included: Bool
            do {
                included = try predicate(input)
            } catch {
                fail(with: error)
                return .none
            }
            guard included else { return .max(1) }
            return downstream.receive(input)
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

        private func fail(with error: any Error) {
            lock.lock()
            let subscription = status.subscription
            status = .terminal
            lock.unlock()
            subscription?.cancel()
            downstream.receive(completion: .failure(error))
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
            switch status {
            case .awaitingSubscription:
                // Cancelled before the upstream subscription arrived: mark
                // terminal so a later subscription is cancelled on arrival
                // (official behavior verified by the parity suite).
                status = .terminal
                lock.unlock()
            case let .subscribed(subscription):
                status = .terminal
                lock.unlock()
                subscription.cancel()
            case .pendingTerminal, .terminal:
                lock.unlock()
            }
        }

        var description: String { "TryFilter" }
        var customMirror: Mirror {
            Mirror(self, children: [("downstream", downstream as Any)])
        }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Republishes elements that pass a test.
    public func filter(_ isIncluded: @escaping (Output) -> Bool) -> Publishers.Filter<Self> {
        .init(upstream: self, isIncluded: isIncluded)
    }

    /// Republishes elements that pass an error-throwing test.
    public func tryFilter(_ isIncluded: @escaping (Output) throws -> Bool)
    -> Publishers.TryFilter<Self> {
        .init(upstream: self, isIncluded: isIncluded)
    }
}

extension Publishers.Filter {

    /// Operator specialization: composes predicates with short-circuiting
    /// `&&` over one upstream (verified by the parity suite's call counts).
    public func filter(_ isIncluded: @escaping (Output) -> Bool)
    -> Publishers.Filter<Upstream> {
        .init(upstream: upstream) { self.isIncluded($0) && isIncluded($0) }
    }

    public func tryFilter(_ isIncluded: @escaping (Output) throws -> Bool)
    -> Publishers.TryFilter<Upstream> {
        .init(upstream: upstream) { try (self.isIncluded($0) && isIncluded($0)) }
    }
}

extension Publishers.TryFilter {

    /// Operator specialization: stays a `TryFilter`, composing predicates
    /// with short-circuiting `&&`.
    public func filter(_ isIncluded: @escaping (Output) -> Bool)
    -> Publishers.TryFilter<Upstream> {
        .init(upstream: upstream) { try (self.isIncluded($0) && isIncluded($0)) }
    }

    public func tryFilter(_ isIncluded: @escaping (Output) throws -> Bool)
    -> Publishers.TryFilter<Upstream> {
        .init(upstream: upstream) { try (self.isIncluded($0) && isIncluded($0)) }
    }
}
