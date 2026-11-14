extension Publishers {

    /// A publisher that publishes an optional value and republishes all
    /// non-`nil` results.
    ///
    /// Official API: `struct CompactMap<Upstream, Output>`.
    public struct CompactMap<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let transform: (Upstream.Output) -> Output?

        public init(upstream: Upstream, transform: @escaping (Upstream.Output) -> Output?) {
            self.upstream = upstream
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, transform: transform))
        }
    }

    /// Official API: `struct TryCompactMap<Upstream, Output>`.
    public struct TryCompactMap<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Error

        public let upstream: Upstream
        public let transform: (Upstream.Output) throws -> Output?

        public init(
            upstream: Upstream,
            transform: @escaping (Upstream.Output) throws -> Output?
        ) {
            self.upstream = upstream
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, transform: transform))
        }
    }
}

extension Publishers.CompactMap {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let transform: (Input) -> Output?
        let combineIdentifier = CombineIdentifier()

        fileprivate init(downstream: Downstream, transform: @escaping (Input) -> Output?) {
            self.downstream = downstream
            self.transform = transform
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            guard let output = transform(input) else { return .max(1) }
            return downstream.receive(output)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "CompactMap" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryCompactMap {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let transform: (Input) throws -> Output?
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(downstream: Downstream, transform: @escaping (Input) throws -> Output?) {
            self.downstream = downstream
            self.transform = transform
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

            let output: Output?
            do {
                output = try transform(input)
            } catch {
                lock.lock()
                let subscription = status.subscription
                status = .terminal
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
            guard let output else { return .max(1) }
            return downstream.receive(output)
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

        var description: String { "TryCompactMap" }
        var customMirror: Mirror {
            Mirror(self, children: [("downstream", downstream as Any)])
        }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Publishes the non-`nil` results of transforming each element.
    public func compactMap<T>(_ transform: @escaping (Output) -> T?)
    -> Publishers.CompactMap<Self, T> {
        .init(upstream: self, transform: transform)
    }

    /// Publishes the non-`nil` results of an error-throwing transform.
    public func tryCompactMap<T>(_ transform: @escaping (Output) throws -> T?)
    -> Publishers.TryCompactMap<Self, T> {
        .init(upstream: self, transform: transform)
    }
}

extension Publishers.CompactMap {

    /// Operator specialization: composes over one upstream, preserving the
    /// nil-filtering behavior (verified by the parity suite).
    public func compactMap<T>(_ transform: @escaping (Output) -> T?)
    -> Publishers.CompactMap<Upstream, T> {
        .init(upstream: upstream) { input in
            self.transform(input).flatMap(transform)
        }
    }

    public func map<T>(_ transform: @escaping (Output) -> T)
    -> Publishers.CompactMap<Upstream, T> {
        .init(upstream: upstream) { input in
            guard let value = self.transform(input) else { return nil }
            return transform(value)
        }
    }
}

extension Publishers.TryCompactMap {

    /// Operator specialization: composes over one upstream, staying throwing.
    public func compactMap<T>(_ transform: @escaping (Output) -> T?)
    -> Publishers.TryCompactMap<Upstream, T> {
        .init(upstream: upstream) { input in
            guard let value = try self.transform(input) else { return nil }
            return transform(value)
        }
    }
}
