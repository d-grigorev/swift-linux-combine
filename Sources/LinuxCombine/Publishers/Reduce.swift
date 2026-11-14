extension Publishers {

    /// A publisher that publishes a single result after reducing all of the
    /// upstream publisher's elements.
    ///
    /// Official API: `struct Reduce<Upstream, Output>`.
    ///
    /// Semantics (verified by the parity suite): pulls `.unlimited` from the
    /// upstream immediately; values are swallowed into the running result
    /// (each `receive` returns `.none`); the result and `.finished` are
    /// emitted once the *downstream* has outstanding demand — even if the
    /// upstream completed earlier. Failures are delivered immediately and
    /// terminally; requests before the subscription is established are dropped.
    public struct Reduce<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let initialResult: Output
        public let nextPartialResult: (Output, Upstream.Output) -> Output

        public init(
            upstream: Upstream,
            initialResult: Output,
            nextPartialResult: @escaping (Output, Upstream.Output) -> Output
        ) {
            self.upstream = upstream
            self.initialResult = initialResult
            self.nextPartialResult = nextPartialResult
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                initial: initialResult,
                next: nextPartialResult
            ))
        }
    }

    /// Official API: `struct TryReduce<Upstream, Output>`.
    public struct TryReduce<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Error

        public let upstream: Upstream
        public let initialResult: Output
        public let nextPartialResult: (Output, Upstream.Output) throws -> Output

        public init(
            upstream: Upstream,
            initialResult: Output,
            nextPartialResult: @escaping (Output, Upstream.Output) throws -> Output
        ) {
            self.upstream = upstream
            self.initialResult = initialResult
            self.nextPartialResult = nextPartialResult
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                initial: initialResult,
                next: nextPartialResult
            ))
        }
    }
}

extension Publishers.Reduce {

    /// Mirror children (official): `downstream`, `result`, `initial`, `status`.
    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let initial: Output
        private var result: Output
        private let next: (Output, Input) -> Output
        private var status = SubscriptionStatus.awaitingSubscription
        private var demand = Subscribers.Demand.none
        private var finishedPending = false
        private let lock = Lock()

        fileprivate init(
            downstream: Downstream,
            initial: Output,
            next: @escaping (Output, Input) -> Output
        ) {
            self.downstream = downstream
            self.initial = initial
            self.result = initial
            self.next = next
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
            let current = result
            lock.unlock()
            let new = next(current, input)   // user closure: never under lock
            lock.lock()
            result = new
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
                    let result = self.result
                    status = .terminal
                    lock.unlock()
                    _ = downstream.receive(result)
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
            let result = self.result
            status = .terminal
            lock.unlock()
            _ = downstream.receive(result)
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

        var description: String { "Reduce" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("result", result),
                ("initial", initial),
                ("status", status as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryReduce {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let initial: Output
        private var result: Output
        private let next: (Output, Input) throws -> Output
        private var status = SubscriptionStatus.awaitingSubscription
        private var demand = Subscribers.Demand.none
        private var finishedPending = false
        private let lock = Lock()

        fileprivate init(
            downstream: Downstream,
            initial: Output,
            next: @escaping (Output, Input) throws -> Output
        ) {
            self.downstream = downstream
            self.initial = initial
            self.result = initial
            self.next = next
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
            let current = result
            lock.unlock()
            let new: Output
            do {
                new = try next(current, input)   // user closure: never under lock
            } catch {
                lock.lock()
                let subscription = status.subscription
                status = .terminal
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
            lock.lock()
            result = new
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
                downstream.receive(completion: .failure(error as any Error))
            case .finished:
                if demand > 0 {
                    let result = self.result
                    status = .terminal
                    lock.unlock()
                    _ = downstream.receive(result)
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
            let result = self.result
            status = .terminal
            lock.unlock()
            _ = downstream.receive(result)
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

        var description: String { "TryReduce" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("result", result),
                ("initial", initial),
                ("status", status as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Publishes a single result after reducing all upstream elements.
    public func reduce<T>(
        _ initialResult: T,
        _ nextPartialResult: @escaping (T, Output) -> T
    ) -> Publishers.Reduce<Self, T> {
        .init(upstream: self, initialResult: initialResult, nextPartialResult: nextPartialResult)
    }

    /// Throwing variant of `reduce(_:_:)`.
    public func tryReduce<T>(
        _ initialResult: T,
        _ nextPartialResult: @escaping (T, Output) throws -> T
    ) -> Publishers.TryReduce<Self, T> {
        .init(upstream: self, initialResult: initialResult, nextPartialResult: nextPartialResult)
    }
}
