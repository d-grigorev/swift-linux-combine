extension Publishers {

    /// A publisher that publishes an element only after a specified number
    /// of the same elements have been published.
    ///
    /// Official API: `struct RemoveDuplicates<Upstream>`; the mirror child
    /// `last` holds the previously published element.
    public struct RemoveDuplicates<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let predicate: (Output, Output) -> Bool

        public init(upstream: Upstream, predicate: @escaping (Output, Output) -> Bool) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: predicate))
        }
    }

    /// Official API: `struct TryRemoveDuplicates<Upstream>`.
    public struct TryRemoveDuplicates<Upstream: Publisher>: Publisher {
        public typealias Failure = Error
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let predicate: (Output, Output) throws -> Bool

        public init(
            upstream: Upstream,
            predicate: @escaping (Output, Output) throws -> Bool
        ) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: predicate))
        }
    }
}

extension Publishers.RemoveDuplicates {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input, Input) -> Bool
        private var last: Input?
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(downstream: Downstream, predicate: @escaping (Input, Input) -> Bool) {
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
            let previous = last
            lock.unlock()

            let isDuplicate: Bool
            if let previous {
                isDuplicate = predicate(previous, input)   // user closure: never under lock
            } else {
                isDuplicate = false
            }
            if isDuplicate {
                return .max(1)   // replenish upstream demand for the skipped element
            }
            lock.lock()
            last = input
            lock.unlock()
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

        var description: String { "RemoveDuplicates" }
        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("last", last as Any),
            ]
            return Mirror(self, children: children)
        }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryRemoveDuplicates {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let predicate: (Input, Input) throws -> Bool
        private var last: Input?
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(
            downstream: Downstream,
            predicate: @escaping (Input, Input) throws -> Bool
        ) {
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
            let previous = last
            lock.unlock()

            let isDuplicate: Bool
            do {
                if let previous {
                    isDuplicate = try predicate(previous, input)
                } else {
                    isDuplicate = false
                }
            } catch {
                lock.lock()
                let subscription = status.subscription
                status = .terminal
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
            if isDuplicate {
                return .max(1)
            }
            lock.lock()
            last = input
            lock.unlock()
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

        var description: String { "TryRemoveDuplicates" }
        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("last", last as Any),
            ]
            return Mirror(self, children: children)
        }
        var playgroundDescription: Any { description }
    }
}

extension Publisher where Output: Equatable {
    /// Publishes only the first element to be published for each unique
    /// value (compared with `==`).
    public func removeDuplicates() -> Publishers.RemoveDuplicates<Self> {
        .init(upstream: self) { $0 == $1 }
    }
}

extension Publisher {
    /// Publishes only elements that aren't duplicates of their immediate
    /// predecessor, as determined by the given predicate.
    public func removeDuplicates(
        by predicate: @escaping (Output, Output) -> Bool
    ) -> Publishers.RemoveDuplicates<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Throwing variant of `removeDuplicates(by:)`.
    public func tryRemoveDuplicates(
        by predicate: @escaping (Output, Output) throws -> Bool
    ) -> Publishers.TryRemoveDuplicates<Self> {
        .init(upstream: self, predicate: predicate)
    }
}
