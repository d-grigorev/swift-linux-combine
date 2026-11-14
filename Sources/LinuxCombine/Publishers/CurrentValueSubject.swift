/// A subject that wraps a single value and publishes a new element whenever
/// the value changes.
///
/// Official API verified against Apple DocC: `final class
/// CurrentValueSubject<Output, Failure>`, `init(_:)`, `value` (get/set),
/// `Subject` conformance.
///
/// Semantics (resolved via the sanctioned ambiguity consult): a fresh
/// subscriber receives the *current* value on its first request (demand-
/// gated); live sends without demand are dropped, and a dropped send resets
/// the "delivered current value" state so a later request re-delivers
/// whatever the current value is by then.
public final class CurrentValueSubject<Output, Failure: Error>: Subject {

    private let lock = Lock()
    private var active = true
    private var completion: Subscribers.Completion<Failure>?
    private var downstreams: [ConduitBase<Output, Failure>] = []
    private var currentValue: Output
    private var upstreamSubscriptions: [any Subscription] = []

    /// The value wrapped by this subject, published as a new element
    /// whenever it changes.
    public var value: Output {
        get {
            lock.withLock { currentValue }
        }
        set {
            lock.lock()
            // The stored value updates even after completion; only the send
            // is suppressed by the inactive state.
            currentValue = newValue
            sendValueAndConsumeLock(newValue)
        }
    }

    public init(_ value: Output) {
        self.currentValue = value
    }

    deinit {
        for subscription in upstreamSubscriptions {
            subscription.cancel()
        }
    }

    public func send(subscription: any Subscription) {
        lock.lock()
        upstreamSubscriptions.append(subscription)
        lock.unlock()
        subscription.request(.unlimited)
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        lock.lock()
        if active {
            let conduit = Conduit(parent: self, downstream: subscriber)
            downstreams.append(conduit)
            lock.unlock()
            subscriber.receive(subscription: conduit)
        } else {
            let completion = self.completion!
            lock.unlock()
            subscriber.receive(subscription: Subscriptions.empty)
            subscriber.receive(completion: completion)
        }
    }

    public func send(_ value: Output) {
        lock.lock()
        sendValueAndConsumeLock(value)
    }

    /// Publishes `newValue` as the current value. The subject lock is held.
    private func sendValueAndConsumeLock(_ newValue: Output) {
        guard active else {
            lock.unlock()
            return
        }
        currentValue = newValue
        let snapshot = downstreams
        lock.unlock()
        for conduit in snapshot {
            conduit.offer(newValue)
        }
    }

    public func send(completion: Subscribers.Completion<Failure>) {
        lock.lock()
        guard active else {
            lock.unlock()
            return
        }
        active = false
        self.completion = completion
        let snapshot = downstreams
        downstreams.removeAll()
        lock.unlock()
        for conduit in snapshot {
            conduit.finish(completion: completion)
        }
    }

    fileprivate func disassociate(_ conduit: ConduitBase<Output, Failure>) {
        lock.lock()
        guard active else {
            lock.unlock()
            return
        }
        downstreams.removeAll { $0 === conduit }
        lock.unlock()
    }
}

extension CurrentValueSubject {

    private final class Conduit<Downstream: Subscriber>
        : ConduitBase<Output, Failure>, Subscription,
          CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
        where Downstream.Input == Output, Downstream.Failure == Failure
    {
        private var parent: CurrentValueSubject?
        private var downstream: Downstream?
        private var demand = Subscribers.Demand.none
        private var deliveredCurrentValue = false
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()

        fileprivate init(parent: CurrentValueSubject, downstream: Downstream) {
            self.parent = parent
            self.downstream = downstream
        }

        override func offer(_ output: Output) {
            lock.lock()
            guard demand > 0, let downstream else {
                // Dropped for lack of demand: remember that the next request
                // must (re-)deliver the then-current value.
                deliveredCurrentValue = false
                lock.unlock()
                return
            }
            demand -= 1
            deliveredCurrentValue = true
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(output)
            downstreamLock.unlock()
            guard newDemand > 0 else { return }
            lock.lock()
            demand += newDemand
            lock.unlock()
        }

        override func finish(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard let downstream else {
                lock.unlock()
                return
            }
            self.downstream = nil
            let parent = self.parent
            self.parent = nil
            lock.unlock()
            parent?.disassociate(self)
            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            guard let downstream else {
                lock.unlock()
                return
            }
            if deliveredCurrentValue {
                self.demand += demand
                lock.unlock()
                return
            }

            // Hasn't yet delivered the current value: this request is owed it.
            self.demand += demand
            deliveredCurrentValue = true
            if let currentValue = parent?.value {
                self.demand -= 1
                lock.unlock()
                downstreamLock.lock()
                let newDemand = downstream.receive(currentValue)
                downstreamLock.unlock()
                guard newDemand > 0 else { return }
                lock.lock()
                self.demand += newDemand
                lock.unlock()
                return
            }
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            guard downstream != nil else {
                lock.unlock()
                return
            }
            downstream = nil
            let parent = self.parent
            self.parent = nil
            lock.unlock()
            parent?.disassociate(self)
        }

        var description: String { "CurrentValueSubject" }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("parent", parent as Any),
                ("downstream", downstream as Any),
                ("demand", demand),
                ("subject", parent as Any),
            ]
            return Mirror(self, children: children)
        }
    }
}
