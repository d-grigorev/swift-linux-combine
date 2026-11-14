extension Publishers {

    /// A publisher that merges two publishers' elements into a single stream.
    ///
    /// Official API: `struct Merge<A, B>` with equal `Failure`/`Output`.
    ///
    /// Merge semantics note: OpenCombine and the borrowed suite provide no
    /// oracle for merge (those sources/tests don't exist upstream), so these
    /// are implemented from the official contract: all upstreams are pulled
    /// `.unlimited`, emissions are gated by outstanding downstream demand
    /// (values arriving without demand are dropped — the same documented
    /// drop-on-zero-demand rule as `PassthroughSubject`), the stream finishes
    /// when **all** upstreams have finished, and the first failure cancels
    /// the siblings and terminates.
    public struct Merge<A: Publisher, B: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output {
        public typealias Output = A.Output
        public typealias Failure = A.Failure

        public let a: A
        public let b: B

        public init(_ a: A, _ b: B) {
            self.a = a
            self.b = b
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            inner.setChildren([childA, childB])
            a.subscribe(childA)
            b.subscribe(childB)
        }
    }

    /// A publisher that merges three publishers' elements into a single stream.
    public struct Merge3<A: Publisher, B: Publisher, C: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output {
        public typealias Output = A.Output
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
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge3")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            inner.setChildren([childA, childB, childC])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
        }
    }

    /// A publisher that merges four publishers' elements into a single stream.
    public struct Merge4<A: Publisher, B: Publisher, C: Publisher, D: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output,
          C.Failure == D.Failure, C.Output == D.Output {
        public typealias Output = A.Output
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
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge4")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            let childD = MergeChildSubscriber<D, S>(inner: inner, index: 3)
            inner.setChildren([childA, childB, childC, childD])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
        }
    }

    /// A publisher that merges many instances of the same publisher type.
    ///
    /// Official API: `struct MergeMany<Upstream>` (produced by
    /// `merge(with:)` on two same-typed publishers).
    public struct MergeMany<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publishers whose elements are interleaved.
        public let publishers: [Upstream]

        public init(_ publishers: [Upstream]) {
            self.publishers = publishers
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "MergeMany")
            let children = publishers.enumerated().map { index, upstream in
                MergeChildSubscriber<Upstream, S>(inner: inner, index: index)
            }
            inner.setChildren(children)
            for (index, upstream) in publishers.enumerated() {
                upstream.subscribe(children[index])
            }
        }
    }
}


extension Publishers {

    /// A publisher that merges 5 publishers' elements into a single stream.
    public struct Merge5<A: Publisher, B: Publisher, C: Publisher, D: Publisher, E: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output,
          C.Failure == D.Failure, C.Output == D.Output,
          D.Failure == E.Failure, D.Output == E.Output {
        public typealias Output = A.Output
        public typealias Failure = A.Failure

        public let a: A;
        public let b: B;
        public let c: C;
        public let d: D;
        public let e: E;

        public init(_ a: A, _ b: B, _ c: C, _ d: D, _ e: E) {
            self.a = a
            self.b = b
            self.c = c
            self.d = d
            self.e = e
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge5")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            let childD = MergeChildSubscriber<D, S>(inner: inner, index: 3)
            let childE = MergeChildSubscriber<E, S>(inner: inner, index: 4)
            inner.setChildren([childA, childB, childC, childD, childE])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
            e.subscribe(childE)
        }
    }

    /// A publisher that merges 6 publishers' elements into a single stream.
    public struct Merge6<A: Publisher, B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output,
          C.Failure == D.Failure, C.Output == D.Output,
          D.Failure == E.Failure, D.Output == E.Output,
          E.Failure == F.Failure, E.Output == F.Output {
        public typealias Output = A.Output
        public typealias Failure = A.Failure

        public let a: A;
        public let b: B;
        public let c: C;
        public let d: D;
        public let e: E;
        public let f: F;

        public init(_ a: A, _ b: B, _ c: C, _ d: D, _ e: E, _ f: F) {
            self.a = a
            self.b = b
            self.c = c
            self.d = d
            self.e = e
            self.f = f
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge6")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            let childD = MergeChildSubscriber<D, S>(inner: inner, index: 3)
            let childE = MergeChildSubscriber<E, S>(inner: inner, index: 4)
            let childF = MergeChildSubscriber<F, S>(inner: inner, index: 5)
            inner.setChildren([childA, childB, childC, childD, childE, childF])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
            e.subscribe(childE)
            f.subscribe(childF)
        }
    }

    /// A publisher that merges 7 publishers' elements into a single stream.
    public struct Merge7<A: Publisher, B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher, G: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output,
          C.Failure == D.Failure, C.Output == D.Output,
          D.Failure == E.Failure, D.Output == E.Output,
          E.Failure == F.Failure, E.Output == F.Output,
          F.Failure == G.Failure, F.Output == G.Output {
        public typealias Output = A.Output
        public typealias Failure = A.Failure

        public let a: A;
        public let b: B;
        public let c: C;
        public let d: D;
        public let e: E;
        public let f: F;
        public let g: G;

        public init(_ a: A, _ b: B, _ c: C, _ d: D, _ e: E, _ f: F, _ g: G) {
            self.a = a
            self.b = b
            self.c = c
            self.d = d
            self.e = e
            self.f = f
            self.g = g
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge7")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            let childD = MergeChildSubscriber<D, S>(inner: inner, index: 3)
            let childE = MergeChildSubscriber<E, S>(inner: inner, index: 4)
            let childF = MergeChildSubscriber<F, S>(inner: inner, index: 5)
            let childG = MergeChildSubscriber<G, S>(inner: inner, index: 6)
            inner.setChildren([childA, childB, childC, childD, childE, childF, childG])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
            e.subscribe(childE)
            f.subscribe(childF)
            g.subscribe(childG)
        }
    }

    /// A publisher that merges 8 publishers' elements into a single stream.
    public struct Merge8<A: Publisher, B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher, G: Publisher, H: Publisher>: Publisher
    where A.Failure == B.Failure, A.Output == B.Output,
          B.Failure == C.Failure, B.Output == C.Output,
          C.Failure == D.Failure, C.Output == D.Output,
          D.Failure == E.Failure, D.Output == E.Output,
          E.Failure == F.Failure, E.Output == F.Output,
          F.Failure == G.Failure, F.Output == G.Output,
          G.Failure == H.Failure, G.Output == H.Output {
        public typealias Output = A.Output
        public typealias Failure = A.Failure

        public let a: A;
        public let b: B;
        public let c: C;
        public let d: D;
        public let e: E;
        public let f: F;
        public let g: G;
        public let h: H;

        public init(_ a: A, _ b: B, _ c: C, _ d: D, _ e: E, _ f: F, _ g: G, _ h: H) {
            self.a = a
            self.b = b
            self.c = c
            self.d = d
            self.e = e
            self.f = f
            self.g = g
            self.h = h
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = MergeCore<S>(downstream: subscriber, name: "Merge8")
            let childA = MergeChildSubscriber<A, S>(inner: inner, index: 0)
            let childB = MergeChildSubscriber<B, S>(inner: inner, index: 1)
            let childC = MergeChildSubscriber<C, S>(inner: inner, index: 2)
            let childD = MergeChildSubscriber<D, S>(inner: inner, index: 3)
            let childE = MergeChildSubscriber<E, S>(inner: inner, index: 4)
            let childF = MergeChildSubscriber<F, S>(inner: inner, index: 5)
            let childG = MergeChildSubscriber<G, S>(inner: inner, index: 6)
            let childH = MergeChildSubscriber<H, S>(inner: inner, index: 7)
            inner.setChildren([childA, childB, childC, childD, childE, childF, childG, childH])
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
            e.subscribe(childE)
            f.subscribe(childF)
            g.subscribe(childG)
            h.subscribe(childH)
        }
    }

}

// MARK: - Shared merge machinery

/// One upstream side of a merge.
private protocol MergeChild: AnyObject {
    var hasSubscription: Bool { get }
    var childIndex: Int { get }
    func requestUnlimited()
    func cancelUpstream()
}

private final class MergeChildSubscriber<Upstream: Publisher, Downstream: Subscriber>
    : MergeChild, Subscriber, Subscription,
      CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
where Upstream.Output == Downstream.Input, Upstream.Failure == Downstream.Failure {
    typealias Input = Upstream.Output
    typealias Failure = Upstream.Failure

    private let inner: MergeCore<Downstream>
    fileprivate let childIndex: Int
    private var subscription: (any Subscription)?

    fileprivate init(inner: MergeCore<Downstream>, index: Int) {
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

    // MARK: Subscription (what the downstream would use — not granted to it)

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
        inner.childValue(input, from: self)
        return .none   // children are pulled .unlimited
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        inner.childCompleted(completion, from: self)
    }

    var description: String { inner.description }
    var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
    var playgroundDescription: Any { description }
}

/// Shared interleave/demand/completion state machine.
private final class MergeCore<Downstream: Subscriber>:
    Subscription, CustomStringConvertible, CustomReflectable,
    CustomPlaygroundDisplayConvertible
where Downstream.Failure: Error {
    private let lock = Lock()
    private let downstream: Downstream
    private let name: String
    private var children: [any MergeChild] = []
    private var allArrived = false
    private var finishedCount = 0
    private var terminated = false
    private var downstreamDemand = Subscribers.Demand.none
    private var granted = false

    fileprivate init(downstream: Downstream, name: String) {
        self.downstream = downstream
        self.name = name
    }

    fileprivate func setChildren(_ children: [any MergeChild]) {
        lock.lock()
        self.children = children
        lock.unlock()
    }

    fileprivate func childArrived(_ child: any MergeChild) {
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
        // Grant first (the callback may observe upstream history), then start
        // pulling every child.
        downstream.receive(subscription: self)
        lock.lock()
        let children = self.children
        lock.unlock()
        children.forEach { $0.requestUnlimited() }
    }

    fileprivate func childValue(_ input: Downstream.Input, from child: any MergeChild) {
        lock.lock()
        guard !terminated else {
            lock.unlock()
            return
        }
        guard downstreamDemand > 0 else {
            // Documented drop-on-zero-demand behavior.
            lock.unlock()
            return
        }
        downstreamDemand -= 1
        lock.unlock()
        let newDemand = downstream.receive(input)
        guard newDemand > 0 else { return }
        lock.lock()
        downstreamDemand += newDemand
        lock.unlock()
    }

    fileprivate func childCompleted(
        _ completion: Subscribers.Completion<Downstream.Failure>,
        from child: any MergeChild
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
            finishedCount += 1
            let allFinished = finishedCount == children.count
            if allFinished {
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
}

// MARK: - merge methods (official overload set)

extension Publisher {
    /// Merges this publisher with another of the same type.
    public func merge(with other: Self) -> Publishers.MergeMany<Self> {
        .init([self, other])
    }

    /// Merges this publisher with a different publisher of equal output.
    public func merge<P: Publisher>(with other: P) -> Publishers.Merge<Self, P>
    where P.Failure == Failure, P.Output == Output {
        .init(self, other)
    }

    /// Merges this publisher with two others.
    public func merge<B: Publisher, C: Publisher>(with b: B, _ c: C)
    -> Publishers.Merge3<Self, B, C>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output {
        .init(self, b, c)
    }

    /// Merges this publisher with three others.
    public func merge<B: Publisher, C: Publisher, D: Publisher>(with b: B, _ c: C, _ d: D)
    -> Publishers.Merge4<Self, B, C, D>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output,
          D.Failure == Failure, D.Output == Output {
        .init(self, b, c, d)
    }

    /// Merges this publisher with four others.
    public func merge<B: Publisher, C: Publisher, D: Publisher, E: Publisher>(
        with b: B, _ c: C, _ d: D, _ e: E
    ) -> Publishers.Merge5<Self, B, C, D, E>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output,
          D.Failure == Failure, D.Output == Output,
          E.Failure == Failure, E.Output == Output {
        .init(self, b, c, d, e)
    }

    /// Merges this publisher with five others.
    public func merge<B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher>(
        with b: B, _ c: C, _ d: D, _ e: E, _ f: F
    ) -> Publishers.Merge6<Self, B, C, D, E, F>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output,
          D.Failure == Failure, D.Output == Output,
          E.Failure == Failure, E.Output == Output,
          F.Failure == Failure, F.Output == Output {
        .init(self, b, c, d, e, f)
    }

    /// Merges this publisher with six others.
    public func merge<B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher,
                      G: Publisher>(
        with b: B, _ c: C, _ d: D, _ e: E, _ f: F, _ g: G
    ) -> Publishers.Merge7<Self, B, C, D, E, F, G>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output,
          D.Failure == Failure, D.Output == Output,
          E.Failure == Failure, E.Output == Output,
          F.Failure == Failure, F.Output == Output,
          G.Failure == Failure, G.Output == Output {
        .init(self, b, c, d, e, f, g)
    }

    /// Merges this publisher with seven others.
    public func merge<B: Publisher, C: Publisher, D: Publisher, E: Publisher, F: Publisher,
                      G: Publisher, H: Publisher>(
        with b: B, _ c: C, _ d: D, _ e: E, _ f: F, _ g: G, _ h: H
    ) -> Publishers.Merge8<Self, B, C, D, E, F, G, H>
    where B.Failure == Failure, B.Output == Output,
          C.Failure == Failure, C.Output == Output,
          D.Failure == Failure, D.Output == Output,
          E.Failure == Failure, E.Output == Output,
          F.Failure == Failure, F.Output == Output,
          G.Failure == Failure, G.Output == Output,
          H.Failure == Failure, H.Output == Output {
        .init(self, b, c, d, e, f, g, h)
    }
}
