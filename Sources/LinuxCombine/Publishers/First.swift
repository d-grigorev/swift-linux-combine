extension Publishers {

    /// A publisher that publishes the first element of an upstream publisher.
    ///
    /// Official API: `struct First<Upstream>`.
    ///
    /// Semantics (verified by the parity suite): pulls `.unlimited` from the
    /// upstream immediately; the first element is stored (later ones are
    /// dropped) and emitted together with `.finished` as soon as the
    /// downstream has outstanding demand — cancelling the still-live upstream
    /// at that moment. An upstream that finishes without elements emits
    /// `.finished` on demand. Failures are immediate and terminal.
    public struct First<Upstream: Publisher>: Publisher {
        public typealias Failure = Upstream.Failure
        public typealias Output = Upstream.Output

        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(FirstRelay<Upstream, S>(
                downstream: subscriber,
                match: { _ in true },
                selfDescription: "First",
                mapCompletion: { $0 },
                mapThrown: { $0 as! Upstream.Failure }
            ))
        }
    }

    /// A publisher that publishes the first element satisfying a predicate.
    ///
    /// Official API: `struct FirstWhere<Upstream>`.
    /// Its inner subscription describes itself as `"TryFirst"` — official
    /// behavior pinned by the parity suite.
    public struct FirstWhere<Upstream: Publisher>: Publisher {
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
            upstream.subscribe(FirstRelay<Upstream, S>(
                downstream: subscriber,
                match: predicate,
                selfDescription: "TryFirst",
                mapCompletion: { $0 },
                mapThrown: { $0 as! Upstream.Failure }
            ))
        }
    }

    /// A publisher that publishes the first element satisfying a throwing
    /// predicate (terminating with the thrown error).
    ///
    /// Official API: `struct TryFirstWhere<Upstream>`.
    public struct TryFirstWhere<Upstream: Publisher>: Publisher {
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
            upstream.subscribe(FirstRelay<Upstream, S>(
                downstream: subscriber,
                match: predicate,
                selfDescription: "TryFirstWhere",
                mapCompletion: { completion in
                    switch completion {
                    case .finished: return .finished
                    case .failure(let error): return .failure(error as any Error)
                    }
                },
                mapThrown: { $0 }
            ))
        }
    }
}

/// Shared first-element machinery (First / FirstWhere / TryFirstWhere).
///
/// Mirror children (official, via `reduceLikeOperatorMirror`): `downstream`,
/// `result`, `initial`, `status`.
internal final class FirstRelay<Upstream: Publisher, Downstream: Subscriber>
    : Subscriber, Subscription,
      CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
where Downstream.Input == Upstream.Output
{
    typealias Input = Upstream.Output
    typealias Failure = Upstream.Failure

    private let downstream: Downstream
    private let match: (Input) throws -> Bool
    private let mapCompletion: (Subscribers.Completion<Upstream.Failure>)
        -> Subscribers.Completion<Downstream.Failure>
    /// Maps a *thrown* (not upstream) error into the downstream failure type.
    /// Unreachable for non-throwing predicates (plain First/FirstWhere).
    private let mapThrown: (any Error) -> Downstream.Failure
    private let selfDescription: String
    private var result: Input?
    private var status = SubscriptionStatus.awaitingSubscription
    private var demand = Subscribers.Demand.none
    private var finishedPending = false
    private let lock = Lock()

    init(
        downstream: Downstream,
        match: @escaping (Input) throws -> Bool,
        selfDescription: String,
        mapCompletion: @escaping (Subscribers.Completion<Upstream.Failure>)
            -> Subscribers.Completion<Downstream.Failure>,
        mapThrown: @escaping (any Error) -> Downstream.Failure
    ) {
        self.downstream = downstream
        self.match = match
        self.selfDescription = selfDescription
        self.mapCompletion = mapCompletion
        self.mapThrown = mapThrown
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
        // Grant the downstream subscription FIRST (its callback may observe
        // upstream history), THEN pull .unlimited — official order verified
        // by the parity suite.
        downstream.receive(subscription: self)
        subscription.request(.unlimited)
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        lock.lock()
        guard case .subscribed = status, !finishedPending, result == nil else {
            lock.unlock()
            return .none
        }
        lock.unlock()

        let matches: Bool
        do {
            matches = try match(input)   // user closure: never under lock
        } catch {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return .none
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
            downstream.receive(completion: .failure(mapThrown(error)))
            return .none
        }
        guard matches else { return .none }

        lock.lock()
        guard case let .subscribed(subscription) = status, result == nil else {
            lock.unlock()
            return .none
        }
        result = input
        guard demand > 0 else {
            lock.unlock()
            return .none
        }
        status = .terminal
        lock.unlock()
        _ = downstream.receive(input)
        downstream.receive(completion: .finished)
        subscription.cancel()
        return .none
    }

    func receive(completion: Subscribers.Completion<Upstream.Failure>) {
        lock.lock()
        guard case .subscribed = status, !finishedPending else {
            lock.unlock()
            return
        }
        switch completion {
        case .failure:
            status = .terminal
            lock.unlock()
            downstream.receive(completion: mapCompletion(completion))
        case .finished:
            if demand > 0 {
                let value = result
                status = .terminal
                lock.unlock()
                if let value {
                    _ = downstream.receive(value)
                }
                downstream.receive(completion: .finished)
            } else {
                finishedPending = true
                lock.unlock()
            }
        }
    }

    func request(_ demand: Subscribers.Demand) {
        lock.lock()
        guard case let .subscribed(subscription) = status else {
            lock.unlock()
            return
        }
        self.demand += demand
        let ready = (finishedPending || result != nil) && self.demand > 0
        guard ready else {
            lock.unlock()
            return
        }
        let value = result
        let wasUpstreamFinished = finishedPending
        status = .terminal
        lock.unlock()
        if let value {
            _ = downstream.receive(value)
        }
        downstream.receive(completion: .finished)
        if !wasUpstreamFinished {
            subscription.cancel()
        }
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

    var description: String { selfDescription }

    var customMirror: Mirror {
        lock.lock()
        defer { lock.unlock() }
        let children: [Mirror.Child] = [
            ("downstream", downstream as Any),
            ("result", result as Any),
            ("initial", Optional<Input>.none as Any),
            ("status", status as Any),
        ]
        return Mirror(self, children: children)
    }

    var playgroundDescription: Any { description }
}

extension Publisher {
    /// Publishes the first element of the upstream publisher.
    public func first() -> Publishers.First<Self> {
        .init(upstream: self)
    }

    /// Publishes the first element satisfying the predicate.
    public func first(where predicate: @escaping (Output) -> Bool)
    -> Publishers.FirstWhere<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Publishes the first element satisfying a throwing predicate.
    public func tryFirst(where predicate: @escaping (Output) throws -> Bool)
    -> Publishers.TryFirstWhere<Self> {
        .init(upstream: self, predicate: predicate)
    }
}
