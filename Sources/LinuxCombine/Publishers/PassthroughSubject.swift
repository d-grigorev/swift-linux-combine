/// A subject that broadcasts elements to downstream subscribers.
///
/// Official API verified against Apple DocC: `final class
/// PassthroughSubject<Output, Failure>`, `init()`, `Subject` conformance
/// (`send(_:)`, `send(completion:)`, `send(subscription:)`).
///
/// Documented behavior (Apple docs): "A PassthroughSubject drops values if
/// there are no subscribers, or its current demand is zero."
///
/// Demand model for relayed upstreams (`subscribe(subject)` wiring): as soon
/// as any downstream requests elements, the subject requests `.unlimited`
/// from every subscription passed via `send(subscription:)` — per-subscriber
/// demand gating happens at the conduit.
public final class PassthroughSubject<Output, Failure: Error>: Subject {

    private let lock = Lock()
    private var active = true
    private var completion: Subscribers.Completion<Failure>?
    private var downstreams: [ConduitBase<Output, Failure>] = []
    private var upstreamSubscriptions: [any Subscription] = []
    private var hasAnyDownstreamDemand = false

    public init() {}

    deinit {
        for subscription in upstreamSubscriptions {
            subscription.cancel()
        }
    }

    public func send(subscription: any Subscription) {
        lock.lock()
        upstreamSubscriptions.append(subscription)
        let hasDemand = hasAnyDownstreamDemand
        lock.unlock()
        if hasDemand {
            subscription.request(.unlimited)
        }
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
        guard active else {
            lock.unlock()
            return
        }
        let snapshot = downstreams
        lock.unlock()
        for conduit in snapshot {
            conduit.offer(value)
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

    private func acknowledgeDownstreamDemand() {
        lock.lock()
        if hasAnyDownstreamDemand {
            lock.unlock()
            return
        }
        hasAnyDownstreamDemand = true
        let subscriptions = upstreamSubscriptions
        lock.unlock()
        for subscription in subscriptions {
            subscription.request(.unlimited)
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

extension PassthroughSubject {

    private final class Conduit<Downstream: Subscriber>
        : ConduitBase<Output, Failure>, Subscription,
          CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
        where Downstream.Input == Output, Downstream.Failure == Failure
    {
        private var parent: PassthroughSubject?
        private var downstream: Downstream?
        private var demand = Subscribers.Demand.none
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()

        fileprivate init(parent: PassthroughSubject, downstream: Downstream) {
            self.parent = parent
            self.downstream = downstream
        }

        override func offer(_ output: Output) {
            lock.lock()
            guard demand > 0, let downstream else {
                // No demand: the element is dropped (documented behavior).
                lock.unlock()
                return
            }
            demand -= 1
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
            guard downstream != nil else {
                lock.unlock()
                return
            }
            self.demand += demand
            let parent = self.parent
            lock.unlock()
            parent?.acknowledgeDownstreamDemand()
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

        var description: String { "PassthroughSubject" }

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
