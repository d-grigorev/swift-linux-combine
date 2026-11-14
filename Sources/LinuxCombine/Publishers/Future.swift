/// A publisher that either fails with the specified error or provides a
/// single value *and then finishes*.
///
/// Official API verified against Apple DocC: `final class
/// Future<Output, Failure>`, `typealias Promise`,
/// `init(_ attemptToFulfill:)`, `value` (async throws; async non-throwing
/// for `Failure == Never`).
///
/// Semantics (resolved via the sanctioned ambiguity consult): the future is
/// **eager** — the attempt closure runs once, at init. The first promise
/// wins; later invocations are ignored. Successes wait for downstream
/// demand; failures are delivered as soon as the subscriber has a
/// subscription. Every subscriber receives the stored result.
public final class Future<Output, Failure: Error>: Publisher {

    /// A type that represents a closure to invoke in the future, when an
    /// element or error is available.
    public typealias Promise = (Result<Output, Failure>) -> Void

    private let lock = Lock()
    private var downstreams: [ConduitBase<Output, Failure>] = []
    private var result: Result<Output, Failure>?

    /// Creates a publisher that invokes a promise closure when the publisher
    /// emits an element. The closure runs immediately, exactly once.
    public init(
        _ attemptToFulfill: @escaping (@escaping Promise) -> Void
    ) {
        attemptToFulfill(promise)
    }

    private func promise(_ result: Result<Output, Failure>) {
        lock.lock()
        guard self.result == nil else {
            lock.unlock()
            return
        }
        self.result = result
        let snapshot = downstreams
        downstreams.removeAll()
        lock.unlock()
        switch result {
        case .success(let output):
            for conduit in snapshot { conduit.offer(output) }
        case .failure(let error):
            for conduit in snapshot { conduit.finish(completion: .failure(error)) }
        }
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        let conduit = Conduit(parent: self, downstream: subscriber)
        lock.lock()
        downstreams.append(conduit)
        let result = self.result
        lock.unlock()
        subscriber.receive(subscription: conduit)
        if let result {
            conduit.fulfill(result)
        }
    }

    fileprivate func resultIfResolved() -> Result<Output, Failure>? {
        lock.withLock { result }
    }

    fileprivate func disassociate(_ conduit: ConduitBase<Output, Failure>) {
        lock.lock()
        downstreams.removeAll { $0 === conduit }
        lock.unlock()
    }
}

extension Future {

    private final class Conduit<Downstream: Subscriber>
        : ConduitBase<Output, Failure>, Subscription,
          CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
        where Downstream.Input == Output, Downstream.Failure == Failure
    {
        private enum State {
            case active(Downstream, hasAnyDemand: Bool)
            case terminal
        }

        private var parent: Future?
        private var state: State
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()

        fileprivate init(parent: Future, downstream: Downstream) {
            self.parent = parent
            self.state = .active(downstream, hasAnyDemand: false)
        }

        override func offer(_ output: Output) {
            fulfill(.success(output))
        }

        override func finish(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .failure(let error):
                fulfill(.failure(error))
            case .finished:
                // A Future never publishes `.finished` directly; fulfillment
                // appends it after the value.
                assertionFailure("unreachable")
            }
        }

        /// Delivers the stored result, honoring demand for success.
        ///
        /// Delivery happens *before* the parent link is cleared — observers
        /// inspecting the conduit from inside the value callback still see
        /// `parent` set (verified by the parity suite).
        fileprivate func fulfill(_ result: Result<Output, Failure>) {
            lock.lock()
            guard case .active(let downstream, let hasAnyDemand) = state else {
                lock.unlock()
                return
            }
            if case .success = result, !hasAnyDemand {
                // Success waits for demand; the request path re-invokes
                // fulfillment once demand arrives.
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            deliver(downstream: downstream, result: result)
            releaseParent()
        }

        private func releaseParent() {
            lock.lock()
            let parent = self.parent
            self.parent = nil
            lock.unlock()
            parent?.disassociate(self)
        }

        private func deliver(downstream: Downstream, result: Result<Output, Failure>) {
            downstreamLock.lock()
            switch result {
            case .success(let output):
                _ = downstream.receive(output)
                downstream.receive(completion: .finished)
            case .failure(let error):
                downstream.receive(completion: .failure(error))
            }
            downstreamLock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            guard case .active(let downstream, _) = state else {
                lock.unlock()
                return
            }
            state = .active(downstream, hasAnyDemand: true)
            let result = parent?.resultIfResolved()
            if let result {
                let parent = self.parent
                state = .terminal
                lock.unlock()
                deliver(downstream: downstream, result: result)
                // Deliberately *not* clearing `self.parent` on this path:
                // Apple's implementation keeps the reference (the parity
                // suite inspects the conduit after request-time delivery).
                parent?.disassociate(self)
            } else {
                lock.unlock()
            }
        }

        func cancel() {
            lock.lock()
            guard case .active = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            releaseParent()
        }

        var description: String { "Future" }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let downstream: Downstream?
            let hasAnyDemand: Bool
            switch state {
            case .active(let activeDownstream, let demand):
                downstream = activeDownstream
                hasAnyDemand = demand
            case .terminal:
                downstream = nil
                hasAnyDemand = false
            }
            let children: [Mirror.Child] = [
                ("parent", parent as Any),
                ("downstream", downstream as Any),
                ("hasAnyDemand", hasAnyDemand),
                ("subject", parent as Any),
            ]
            return Mirror(self, children: children)
        }
    }
}

extension Future {

    /// The published value of the future, delivered asynchronously.
    /// Throws the future's failure, if any.
    public var value: Output {
        get async throws {
            let result: Result<Output, any Error> = await withCheckedContinuation { continuation in
                let waiter = FutureWaiter<Output, Failure> { result in
                    continuation.resume(returning: result)
                }
                subscribe(waiter)
            }
            return try result.get()
        }
    }
}

extension Future where Failure == Never {

    /// The published value of the future, delivered asynchronously.
    public var value: Output {
        get async {
            await withCheckedContinuation { (continuation: CheckedContinuation<Output, Never>) in
                let waiter = FutureWaiter<Output, Never> { result in
                    switch result {
                    case .success(let output):
                        continuation.resume(returning: output)
                    case .failure:
                        assertionFailure("Future with Failure == Never failed")
                    }
                }
                subscribe(waiter)
            }
        }
    }
}

/// Internal subscriber that funnels a Future's single result into an async
/// continuation exactly once.
private final class FutureWaiter<Input, Failure: Error>: Subscriber {

    private let lock = Lock()
    private let resume: (Result<Input, any Error>) -> Void
    private var pendingValue: Input?
    private var hasValue = false
    private var resumed = false

    fileprivate init(_ resume: @escaping (Result<Input, any Error>) -> Void) {
        self.resume = resume
    }

    func receive(subscription: any Subscription) {
        // No lock held: request can deliver synchronously.
        subscription.request(.unlimited)
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        lock.lock()
        pendingValue = input
        hasValue = true
        lock.unlock()
        return .none
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        lock.lock()
        guard !resumed else {
            lock.unlock()
            return
        }
        resumed = true
        let value = pendingValue
        let hasValue = self.hasValue
        let resume = self.resume
        lock.unlock()
        switch completion {
        case .finished:
            precondition(hasValue, "Future finished without a value")
            resume(.success(value!))
        case .failure(let error):
            resume(.failure(error))
        }
    }
}
