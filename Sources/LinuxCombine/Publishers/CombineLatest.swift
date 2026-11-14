extension Publishers {

    /// A publisher that combines the latest elements from two publishers.
    ///
    /// Official API: `struct CombineLatest<A, B>` with equal `Failure`.
    ///
    /// Semantics note: OpenCombine provides no CombineLatest (that source
    /// doesn't exist upstream) and the borrowed suite has no oracle either,
    /// so these follow the official contract: an element is emitted whenever
    /// *either* side emits — but only once **both** sides have emitted at
    /// least once — pairing the newest values; all sides are pulled
    /// `.unlimited`; emissions are gated by outstanding downstream demand
    /// (a trigger without demand is dropped — the latest-value state means
    /// the next trigger re-pairs); a failure cancels the sibling and
    /// terminates; downstream finishes only when **both** sides finished.
    public struct CombineLatest<A: Publisher, B: Publisher>: Publisher
    where A.Failure == B.Failure {
        public typealias Output = (A.Output, B.Output)
        public typealias Failure = A.Failure

        public let a: A
        public let b: B

        public init(_ a: A, _ b: B) {
            self.a = a
            self.b = b
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = CombineLatestCore<S>(downstream: subscriber, name: "CombineLatest")
            let childA = CombineLatestChild<A, S>(inner: inner, index: 0)
            let childB = CombineLatestChild<B, S>(inner: inner, index: 1)
            inner.setChildren([childA, childB])
            a.subscribe(childA)
            b.subscribe(childB)
        }
    }

    /// A publisher that combines the latest elements from three publishers.
    public struct CombineLatest3<A: Publisher, B: Publisher, C: Publisher>: Publisher
    where A.Failure == B.Failure, B.Failure == C.Failure {
        public typealias Output = (A.Output, B.Output, C.Output)
        public typealias Failure = A.Failure

        public let a: A
        public let b: B
        public let c: C

        public init(_ a: A, _ b: B, _ c: C) {
            self.a = a
            self.b = b
            self.c = c
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = CombineLatestCore<S>(downstream: subscriber, name: "CombineLatest3")
            let childA = CombineLatestChild<A, S>(inner: inner, index: 0)
            let childB = CombineLatestChild<B, S>(inner: inner, index: 1)
            let childC = CombineLatestChild<C, S>(inner: inner, index: 2)
            inner.setChildren([childA, childB, childC])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
        }
    }

    /// A publisher that combines the latest elements from four publishers.
    public struct CombineLatest4<A: Publisher, B: Publisher, C: Publisher, D: Publisher>
    : Publisher
    where A.Failure == B.Failure, B.Failure == C.Failure, C.Failure == D.Failure {
        public typealias Output = (A.Output, B.Output, C.Output, D.Output)
        public typealias Failure = A.Failure

        public let a: A
        public let b: B
        public let c: C
        public let d: D

        public init(_ a: A, _ b: B, _ c: C, _ d: D) {
            self.a = a
            self.b = b
            self.c = c
            self.d = d
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = CombineLatestCore<S>(downstream: subscriber, name: "CombineLatest4")
            let childA = CombineLatestChild<A, S>(inner: inner, index: 0)
            let childB = CombineLatestChild<B, S>(inner: inner, index: 1)
            let childC = CombineLatestChild<C, S>(inner: inner, index: 2)
            let childD = CombineLatestChild<D, S>(inner: inner, index: 3)
            inner.setChildren([childA, childB, childC, childD])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
        }
    }
}

// MARK: - Shared combineLatest machinery

/// One upstream side: stores the newest value until paired.
private final class CombineLatestChild<Upstream: Publisher, Downstream: Subscriber>
    : Subscriber, Subscription, CustomStringConvertible, CustomReflectable,
      CustomPlaygroundDisplayConvertible
where Upstream.Failure == Downstream.Failure {
    typealias Input = Upstream.Output
    typealias Failure = Upstream.Failure

    private let inner: CombineLatestCore<Downstream>
    private let childIndex: Int
    private var subscription: (any Subscription)?

    /// The newest value, if this side has produced at least one.
    fileprivate var latest: Input?
    fileprivate var hasLatest = false

    fileprivate init(inner: CombineLatestCore<Downstream>, index: Int) {
        self.inner = inner
        self.childIndex = index
    }

    fileprivate var hasSubscription: Bool { subscription != nil }

    fileprivate func requestUnlimited() {
        subscription?.request(.unlimited)
    }

    fileprivate func cancelUpstream() {
        subscription?.cancel()
        subscription = nil
    }

    // MARK: Subscription (not granted downstream)

    func request(_ demand: Subscribers.Demand) {
        subscription?.request(demand)
    }

    func cancel() {
        cancelUpstream()
    }

    // MARK: Subscriber

    func receive(subscription: any Subscription) {
        guard self.subscription == nil else {
            subscription.cancel()
            return
        }
        self.subscription = subscription
        inner.childArrived(self)
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        latest = input
        hasLatest = true
        inner.childUpdated(from: self)
        return .none   // children are pulled .unlimited
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        inner.childCompleted(completion, from: self)
    }

    var description: String { inner.description }
    var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
    var playgroundDescription: Any { description }
}

/// Shared pairing/demand/completion state machine (arity-agnostic: the inner
/// dequeues via the children's `latest` values).
private final class CombineLatestCore<Downstream: Subscriber>:
    Subscription, CustomStringConvertible, CustomReflectable,
    CustomPlaygroundDisplayConvertible {
    private let lock = Lock()
    private let downstream: Downstream
    private let name: String
    private var children: [AnyCombineLatestChild] = []
    private var allArrived = false
    private var finishedCount = 0
    private var terminated = false
    private var granted = false
    private var downstreamDemand = Subscribers.Demand.none

    fileprivate init(downstream: Downstream, name: String) {
        self.downstream = downstream
        self.name = name
    }

    fileprivate func setChildren(_ children: [AnyCombineLatestChild]) {
        lock.lock()
        self.children = children
        lock.unlock()
    }

    fileprivate func childArrived(_ child: AnyCombineLatestChild) {
        lock.lock()
        guard !terminated else {
            lock.unlock()
            child.cancelUpstream()
            return
        }
        guard children.allSatisfy({ $0.hasSubscription }), !granted else {
            lock.unlock()
            return
        }
        granted = true
        lock.unlock()
        // Grant first (callback may observe upstream history), then pull.
        downstream.receive(subscription: self)
        lock.lock()
        let children = self.children
        lock.unlock()
        children.forEach { $0.requestUnlimited() }
    }

    fileprivate func childUpdated(from child: AnyCombineLatestChild) {
        lock.lock()
        guard !terminated, granted else {
            lock.unlock()
            return
        }
        guard children.allSatisfy({ $0.hasLatest }) else {
            lock.unlock()
            return
        }
        guard downstreamDemand > 0 else {
            // Trigger dropped for lack of demand; the latest-value state
            // means the next emission re-pairs.
            lock.unlock()
            return
        }
        downstreamDemand -= 1
        let snapshot = children
        lock.unlock()

        let pair = snapshot.map { $0.takeLatest()! }
        let output: Downstream.Input = flatten(pair)
        let newDemand = downstream.receive(output)
        guard newDemand > 0 else { return }
        lock.lock()
        downstreamDemand += newDemand
        lock.unlock()
    }

    fileprivate func childCompleted(
        _ completion: Subscribers.Completion<Downstream.Failure>,
        from child: AnyCombineLatestChild
    ) {
        switch completion {
        case .failure:
            lock.lock()
            guard !terminated else {
                lock.unlock()
                return
            }
            terminated = true
            let siblings = children
            lock.unlock()
            siblings.forEach { $0.cancelUpstream() }
            downstream.receive(completion: completion)
        case .finished:
            lock.lock()
            guard !terminated else {
                lock.unlock()
                return
            }
            child.markFinished()
            finishedCount += 1
            if finishedCount == children.count {
                terminated = true
                lock.unlock()
                downstream.receive(completion: .finished)
            } else {
                lock.unlock()
            }
        }
    }

    // MARK: Subscription

    func request(_ demand: Subscribers.Demand) {
        assert(demand != .none, "request(_:) demand must not be .none")
        lock.lock()
        downstreamDemand += demand
        lock.unlock()
    }

    func cancel() {
        lock.lock()
        guard !terminated else {
            lock.unlock()
            return
        }
        terminated = true
        let siblings = children
        lock.unlock()
        siblings.forEach { $0.cancelUpstream() }
    }

    var description: String { name }
    var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
    var playgroundDescription: Any { description }

    /// Packs the (type-erased) latest values into the downstream's tuple.
    private func flatten(_ values: [Any]) -> Downstream.Input {
        switch values.count {
        case 2: return (values[0], values[1]) as! Downstream.Input
        case 3: return (values[0], values[1], values[2]) as! Downstream.Input
        case 4: return (values[0], values[1], values[2], values[3]) as! Downstream.Input
        default: preconditionFailure("combineLatest supports 2-4 publishers")
        }
    }
}

/// Type-erased view of one combineLatest side (identity-based coordination).
private protocol AnyCombineLatestChild: AnyObject {
    var hasSubscription: Bool { get }
    var hasLatest: Bool { get }
    func takeLatest() -> Any?
    func markFinished()
    func requestUnlimited()
    func cancelUpstream()
}

extension CombineLatestChild: AnyCombineLatestChild {
    fileprivate func takeLatest() -> Any? {
        // The latest value is NOT consumed: it stays until this side emits
        // again, so subsequent emissions from *other* sides re-pair with it.
        latest.map { $0 as Any }
    }

    fileprivate func markFinished() {}
}

// MARK: - combineLatest methods (official overload set)

extension Publisher {
    /// Combines this publisher's elements with another's, emitting tuples of
    /// the latest values once both have emitted.
    public func combineLatest<P: Publisher>(_ other: P)
    -> Publishers.CombineLatest<Self, P> where P.Failure == Failure {
        .init(self, other)
    }

    /// Combines this publisher with another and transforms the result.
    public func combineLatest<P: Publisher, T>(
        _ other: P,
        _ transform: @escaping (Output, P.Output) -> T
    ) -> Publishers.Map<Publishers.CombineLatest<Self, P>, T>
    where P.Failure == Failure {
        .init(upstream: .init(self, other), transform: transform)
    }

    /// Combines this publisher with two others.
    public func combineLatest<P: Publisher, Q: Publisher>(_ publisher1: P, _ publisher2: Q)
    -> Publishers.CombineLatest3<Self, P, Q>
    where P.Failure == Failure, Q.Failure == Failure {
        .init(self, publisher1, publisher2)
    }

    /// Combines this publisher with two others and transforms the result.
    public func combineLatest<P: Publisher, Q: Publisher, T>(
        _ publisher1: P,
        _ publisher2: Q,
        _ transform: @escaping (Output, P.Output, Q.Output) -> T
    ) -> Publishers.Map<Publishers.CombineLatest3<Self, P, Q>, T>
    where P.Failure == Failure, Q.Failure == Failure {
        .init(upstream: .init(self, publisher1, publisher2), transform: transform)
    }

    /// Combines this publisher with three others.
    public func combineLatest<P: Publisher, Q: Publisher, R: Publisher>(
        _ publisher1: P,
        _ publisher2: Q,
        _ publisher3: R
    ) -> Publishers.CombineLatest4<Self, P, Q, R>
    where P.Failure == Failure, Q.Failure == Failure, R.Failure == Failure {
        .init(self, publisher1, publisher2, publisher3)
    }

    /// Combines this publisher with three others and transforms the result.
    public func combineLatest<P: Publisher, Q: Publisher, R: Publisher, T>(
        _ publisher1: P,
        _ publisher2: Q,
        _ publisher3: R,
        _ transform: @escaping (Output, P.Output, Q.Output, R.Output) -> T
    ) -> Publishers.Map<Publishers.CombineLatest4<Self, P, Q, R>, T>
    where P.Failure == Failure, Q.Failure == Failure, R.Failure == Failure {
        .init(upstream: .init(self, publisher1, publisher2, publisher3), transform: transform)
    }
}
