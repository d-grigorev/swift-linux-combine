extension Publishers {

    /// A publisher that omits elements until a predicate fails.
    ///
    /// Official API: `struct DropWhile<Upstream>` (backing `drop(while:)`).
    public struct DropWhile<Upstream: Publisher>: Publisher {
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

    /// Official API: `struct TryDropWhile<Upstream>` (backing `tryDrop(while:)`).
    public struct TryDropWhile<Upstream: Publisher>: Publisher {
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

extension Publishers.DropWhile {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input) -> Bool
        private var dropping = true
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
            let stillDropping = dropping
            lock.unlock()

            if stillDropping {
                let keepDropping = predicate(input)   // user closure: never under lock
                if keepDropping {
                    return .max(1)   // replenish upstream demand for the skipped element
                }
                lock.lock()
                dropping = false
                lock.unlock()
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
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "DropWhile" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryDropWhile {

    private final class ThrowingInner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input) throws -> Bool
        private var dropping = true
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
            let stillDropping = dropping
            lock.unlock()

            if stillDropping {
                let keepDropping: Bool
                do {
                    keepDropping = try predicate(input)   // user closure: never under lock
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
                if keepDropping {
                    return .max(1)
                }
                lock.lock()
                dropping = false
                lock.unlock()
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
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "TryDropWhile" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Omits elements until the predicate fails, then passes the rest.
    public func drop(while predicate: @escaping (Output) -> Bool)
    -> Publishers.DropWhile<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Throwing variant of `drop(while:)`.
    public func tryDrop(while predicate: @escaping (Output) throws -> Bool)
    -> Publishers.TryDropWhile<Self> {
        .init(upstream: self, predicate: predicate)
    }
}
