extension Publishers {

    /// A publisher that publishes a specified running total for each
    /// element, using a closure.
    ///
    /// Official API: `struct Scan<Upstream, Output>`; mirror children
    /// `downstream`, `result`.
    public struct Scan<Upstream: Publisher, Output>: Publisher {
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

    /// Official API: `struct TryScan<Upstream, Output>`; mirror children
    /// `downstream`, `status`, `result`.
    public struct TryScan<Upstream: Publisher, Output>: Publisher {
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
            upstream.subscribe(Inner(downstream: subscriber, initial: initialResult, next: nextPartialResult))
        }
    }
}

extension Publishers.Scan {

    /// Subscriber-only relay (the upstream subscription passes through to
    /// the downstream), carrying the running result.
    private final class Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let next: (Output, Input) -> Output
        private var result: Output
        private let lock = Lock()

        fileprivate init(downstream: Downstream, initial: Output, next: @escaping (Output, Input) -> Output) {
            self.downstream = downstream
            self.result = initial
            self.next = next
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            let current = result
            lock.unlock()
            let new = next(current, input)   // user closure: never under lock
            lock.lock()
            result = new
            lock.unlock()
            return downstream.receive(new)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "Scan" }
        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("result", result),
            ]
            return Mirror(self, children: children)
        }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.TryScan {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let next: (Output, Input) throws -> Output
        private var result: Output
        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        fileprivate init(
            downstream: Downstream,
            initial: Output,
            next: @escaping (Output, Input) throws -> Output
        ) {
            self.downstream = downstream
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
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            // Official behavior: the value path is blind — the transform runs
            // and the result is forwarded even after completion/cancellation;
            // only the throw path consults status (no double failure).
            lock.lock()
            let current = result
            lock.unlock()
            let new: Output
            do {
                new = try next(current, input)
            } catch {
                lock.lock()
                // Only the first throw fails the stream; later throws (the
                // value path stays blind) are swallowed.
                guard case .subscribed = status else {
                    lock.unlock()
                    return .none
                }
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
            return downstream.receive(new)
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

        var description: String { "TryScan" }
        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream as Any),
                ("status", status as Any),
                ("result", result),
            ]
            return Mirror(self, children: children)
        }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Publishes a specified running total for each element.
    public func scan<T>(
        _ initialResult: T,
        _ nextPartialResult: @escaping (T, Output) -> T
    ) -> Publishers.Scan<Self, T> {
        .init(upstream: self, initialResult: initialResult, nextPartialResult: nextPartialResult)
    }

    /// Throwing variant of `scan(_:_:)`.
    public func tryScan<T>(
        _ initialResult: T,
        _ nextPartialResult: @escaping (T, Output) throws -> T
    ) -> Publishers.TryScan<Self, T> {
        .init(upstream: self, initialResult: initialResult, nextPartialResult: nextPartialResult)
    }
}
