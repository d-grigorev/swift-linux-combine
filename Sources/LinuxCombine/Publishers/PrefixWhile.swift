extension Publishers {

    /// A publisher that republishes elements until a predicate fails.
    ///
    /// Official API: `struct PrefixWhile<Upstream>`; mirror child
    /// `downstream`.
    public struct PrefixWhile<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let predicate: (Output) -> Bool

        public init(upstream: Upstream, predicate: @escaping (Output) -> Bool) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: predicate))
        }
    }

    /// Official API: `struct TryPrefixWhile<Upstream>`.
    public struct TryPrefixWhile<Upstream: Publisher>: Publisher {
        public typealias Failure = Error
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let predicate: (Output) throws -> Bool

        public init(upstream: Upstream, predicate: @escaping (Output) throws -> Bool) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(ThrowingInner(downstream: subscriber, predicate: predicate))
        }
    }
}

extension Publishers.PrefixWhile {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input) -> Bool
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(downstream: Downstream, predicate: @escaping (Input) -> Bool) {
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
            guard case .subscribed = status else {
                lock.unlock()
                return .none
            }
            lock.unlock()

            guard predicate(input) else {   // user closure: never under lock
                // The prefix ended: finish downstream and tear down upstream.
                lock.lock()
                guard case let .subscribed(subscription) = status else {
                    lock.unlock()
                    return .none
                }
                status = .terminal
                lock.unlock()
                subscription.cancel()
                downstream.receive(completion: .finished)
                return .none
            }
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

        var description: String { "PrefixWhile" }
        var customMirror: Mirror {
            Mirror(self, children: [("downstream", downstream as Any)])
        }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryPrefixWhile {

    private final class ThrowingInner<Downstream: Subscriber>: Subscriber, Subscription,
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
            guard case .subscribed = status else {
                lock.unlock()
                return .none
            }
            lock.unlock()

            let passes: Bool
            do {
                passes = try predicate(input)   // user closure: never under lock
            } catch {
                lock.lock()
                guard case let .subscribed(subscription) = status else {
                    lock.unlock()
                    return .none
                }
                status = .terminal
                lock.unlock()
                subscription.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
            guard passes else {
                lock.lock()
                guard case let .subscribed(subscription) = status else {
                    lock.unlock()
                    return .none
                }
                status = .terminal
                lock.unlock()
                subscription.cancel()
                downstream.receive(completion: .finished)
                return .none
            }
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
                downstream.receive(completion: .failure(error))
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

        var description: String { "TryPrefixWhile" }
        var customMirror: Mirror {
            Mirror(self, children: [("downstream", downstream as Any)])
        }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Republishes elements while the predicate holds, then finishes.
    public func prefix(while predicate: @escaping (Output) -> Bool)
    -> Publishers.PrefixWhile<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Throwing variant of `prefix(while:)`.
    public func tryPrefix(while predicate: @escaping (Output) throws -> Bool)
    -> Publishers.TryPrefixWhile<Self> {
        .init(upstream: self, predicate: predicate)
    }
}
