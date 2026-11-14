/// Shared base class for reduce-like operators (`AllSatisfy`, `Contains`,
/// `Count`, `Comparison`, and their try-variants).
///
/// Official behavior verified by the parity suite:
/// - the inner grants *itself* as the subscription to the downstream,
///   then pulls `.unlimited` from the upstream;
/// - values are folded via the overridable `receive(newValue:)`; a fold
///   may end the stream early (`.finished` cancels the upstream and, if
///   the downstream has already requested, delivers the result) or fail
///   (`.failure` cancels the upstream and forwards the error);
/// - the result is held until the downstream makes its first request —
///   upstream completion with no request yet leaves the result pending;
/// - `request(_:)` asserts on `.none` demand and ignores all requests
///   after the first.
///
/// Mirror children (official): `downstream`, `result`, `initial`,
/// `status`.
class ReduceProducer<Downstream: Subscriber, Input, Output, UpstreamFailure: Error,
                     Reducer>
    : Subscriber,
      Subscription,
      CustomStringConvertible,
      CustomReflectable,
      CustomPlaygroundDisplayConvertible
where Downstream.Input == Output {

    /// The outcome of folding one value.
    enum Reduction {
        /// Keep consuming.
        case `continue`
        /// Stop early: cancel the upstream and finish with the result.
        case finished
        /// Stop: cancel the upstream and fail with the error.
        case failure(Downstream.Failure)
    }

    final var result: Output?

    final let initial: Output?

    final let reduce: Reducer

    private var status = SubscriptionStatus.awaitingSubscription

    private let downstream: Downstream

    private let lock = Lock()

    private var downstreamRequested = false

    private var cancelled = false

    private var completed = false

    private var upstreamCompleted = false

    init(downstream: Downstream, initial: Output?, reduce: Reducer) {
        self.downstream = downstream
        self.initial = initial
        self.result = initial
        self.reduce = reduce
    }

    // MARK: - Overridables

    func receive(newValue: Input) -> Reduction {
        fatalError("receive(newValue:) must be overridden")
    }

    var description: String {
        fatalError("description must be overridden")
    }

    // MARK: - Subscriber

    typealias Failure = UpstreamFailure

    func receive(subscription: any Subscription) {
        lock.lock()
        guard case .awaitingSubscription = status else {
            lock.unlock()
            subscription.cancel()
            return
        }
        status = .subscribed(subscription)
        lock.unlock()
        // Grant the downstream FIRST; then pull .unlimited (official
        // order — pinned by the parity lifecycle tests).
        downstream.receive(subscription: self)
        subscription.request(.unlimited)
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        lock.lock()
        guard case .subscribed(let subscription) = status else {
            lock.unlock()
            return .none
        }
        lock.unlock()

        // The user's fold closure runs outside the lock (official
        // behavior; contract violations may race, as upstream).
        switch receive(newValue: input) {
        case .continue:
            break
        case .finished:
            lock.lock()
            upstreamCompleted = true
            let requested = downstreamRequested
            if requested {
                completed = true
            }
            status = .terminal
            let result = self.result
            lock.unlock()
            subscription.cancel()
            guard requested else { break }
            sendResultAndFinish(result)
        case .failure(let error):
            lock.lock()
            upstreamCompleted = true
            completed = true
            status = .terminal
            lock.unlock()
            subscription.cancel()
            downstream.receive(completion: .failure(error))
        }
        return .none
    }

    func receive(completion: Subscribers.Completion<UpstreamFailure>) {
        lock.lock()
        guard case .subscribed = status else {
            lock.unlock()
            return
        }
        status = .terminal
        // The helpers assume the lock is held and unlock partway.
        switch completion {
        case .finished:
            receiveFinished()
        case .failure(let error):
            receiveFailure(error)
        }
    }

    // MARK: - Subscription

    func request(_ demand: Subscribers.Demand) {
        assert(demand != .none, "request(_:) demand must not be .none")
        lock.lock()
        guard !downstreamRequested, !cancelled, !completed else {
            lock.unlock()
            return
        }
        downstreamRequested = true
        guard upstreamCompleted else {
            lock.unlock()
            return
        }
        completed = true
        let result = self.result
        lock.unlock()
        sendResultAndFinish(result)
    }

    func cancel() {
        lock.lock()
        guard case .subscribed(let subscription) = status else {
            lock.unlock()
            return
        }
        cancelled = true
        status = .terminal
        lock.unlock()
        subscription.cancel()
    }

    // MARK: - CustomReflectable

    var customMirror: Mirror {
        lock.lock()
        defer { lock.unlock() }
        let children: [Mirror.Child] = [
            ("downstream", downstream),
            ("result", result as Any),
            ("initial", initial as Any),
            ("status", status),
        ]
        return Mirror(self, children: children)
    }

    var playgroundDescription: Any { description }

    // MARK: - Private

    /// - Precondition: `lock` is held (the caller unlocks partway).
    private func receiveFinished() {
        upstreamCompleted = true
        let requested = downstreamRequested
        if requested {
            completed = true
        }
        let result = self.result
        lock.unlock()
        if requested {
            sendResultAndFinish(result)
        }
        // Otherwise: hold the result until the first request arrives.
    }

    /// - Precondition: `lock` is held (the caller unlocks partway).
    private func receiveFailure(_ failure: UpstreamFailure) {
        upstreamCompleted = true
        completed = true
        lock.unlock()
        // Safe: in every reduce-like operator the downstream failure is
        // either `UpstreamFailure` itself (non-try variants) or `Error`
        // (try-variants).
        downstream.receive(completion: .failure(failure as! Downstream.Failure))
    }

    private func sendResultAndFinish(_ result: Output?) {
        if let result {
            _ = downstream.receive(result)
        }
        downstream.receive(completion: .finished)
    }
}
