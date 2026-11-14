extension Publishers {

    /// A publisher created by applying the zip function to two upstream publishers.
    ///
    /// Official API: `struct Zip<UpstreamA, UpstreamB>` (Failure equality
    /// constrained); `Output` is a 2-tuple.
    ///
    /// Semantics (verified by the parity suite): children are subscribed
    /// eagerly; the downstream subscription is granted once every child has
    /// arrived; each child buffers values and a tuple is emitted when every
    /// child holds one, consuming downstream demand; any upstream finishing
    /// without surplus values ends the stream (cancelling active siblings);
    /// failures propagate immediately. Demand arriving *during* a downstream
    /// delivery is returned from the triggering child's `receive` instead of
    /// a later `request(_:)` (Apple quirk the parity suite pins).
    public struct Zip<UpstreamA: Publisher, UpstreamB: Publisher>: Publisher
    where UpstreamA.Failure == UpstreamB.Failure {
        public typealias Output = (UpstreamA.Output, UpstreamB.Output)
        public typealias Failure = UpstreamA.Failure

        public let a: UpstreamA
        public let b: UpstreamB

        public init(_ a: UpstreamA, _ b: UpstreamB) {
            self.a = a
            self.b = b
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            _ = Inner(downstream: subscriber, a, b)
        }
    }

    /// A publisher created by applying the zip function to three upstream publishers.
    public struct Zip3<A: Publisher, B: Publisher, C: Publisher>: Publisher
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
            _ = Inner(downstream: subscriber, a, b, c)
        }
    }

    /// A publisher created by applying the zip function to four upstream publishers.
    public struct Zip4<A: Publisher, B: Publisher, C: Publisher, D: Publisher>: Publisher
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
            _ = Inner(downstream: subscriber, a, b, c, d)
        }
    }
}

// MARK: - Shared zip machinery

/// One upstream side of a zip: buffers values for the shared pairing logic.
private protocol ZipChild: AnyObject, Subscription {
    var state: ZipChildState { get set }
    var childIndex: Int { get }
    var hasValue: Bool { get }
}

private enum ZipChildState {
    case waitingForSubscription
    case active
    case finished
    case failed
    case canceled
}

private final class ZipChildSubscriber<Upstream: Publisher, Downstream: Subscriber>
    : ZipChild, Subscriber, CustomStringConvertible, CustomReflectable,
      CustomPlaygroundDisplayConvertible
where Upstream.Failure == Downstream.Failure
{
    typealias Input = Upstream.Output
    typealias Failure = Upstream.Failure

    fileprivate var state = ZipChildState.waitingForSubscription
    fileprivate var upstreamSubscription: (any Subscription)?
    fileprivate let childIndex: Int
    private var values: [Upstream.Output] = []
    // `unowned` like the reference implementation: the inner must not be
    // retained by its children or it (and the downstream subscriber) leaks.
    private unowned let parent: ZipCore<Downstream>

    fileprivate init(_ parent: ZipCore<Downstream>, _ childIndex: Int) {
        self.parent = parent
        self.childIndex = childIndex
    }

    fileprivate var hasValue: Bool { !values.isEmpty }

    /// Typed extraction (no `Any` boxing on the hot path).
    fileprivate func takeValue() -> Upstream.Output {
        values.removeFirst()
    }

    // MARK: Subscription

    func request(_ demand: Subscribers.Demand) {
        upstreamSubscription?.request(demand)
    }

    func cancel() {
        upstreamSubscription?.cancel()
        upstreamSubscription = nil
        state = .canceled
    }

    // MARK: Subscriber

    func receive(subscription: any Subscription) {
        if upstreamSubscription == nil {
            upstreamSubscription = subscription
            parent.receivedSubscription(for: self)
        } else {
            subscription.cancel()
        }
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        parent.receivedChildValue(child: self, store: { self.values.append(input) })
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        parent.receivedCompletion(completion, forChild: self)
    }

    var description: String { "Zip" }
    var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
    var playgroundDescription: Any { description }
}

/// Shared pairing/demand state machine for Zip/Zip3/Zip4.
///
/// The main lock is recursive: downstream delivery happens under the lock
/// (matching Apple — re-entrant `request(_:)` from a delivery callback must
/// observe the "value being processed" state).
private class ZipCore<Downstream: Subscriber>:
    Subscription, CustomStringConvertible, CustomReflectable,
    CustomPlaygroundDisplayConvertible
{
    let description = "Zip"

    private let lock = RecursiveLock()
    let downstream: Downstream
    private var downstreamDemand = Subscribers.Demand.none
    private var valueIsBeingProcessed = false
    private var value: Downstream.Input?
    private var isFinished = false
    private var processingValueForChild: (any ZipChild)?
    private var demandReceivedWhileProcessing: Subscribers.Demand?

    init(downstream: Downstream) {
        self.downstream = downstream
    }

    /// Children are fixed at construction; caching the array avoids
    /// rebuilding (and allocating) it on every upstream event.
    fileprivate var cachedChildren: [any ZipChild] = []

    var upstreamSubscriptions: [any ZipChild] { cachedChildren }
    func dequeueValue() -> Downstream.Input { fatalError("abstract") }

    func receivedSubscription(for child: any ZipChild) {
        lock.lock()
        child.state = .active
        let allArrived = upstreamSubscriptions.allSatisfy { $0.state != .waitingForSubscription }
        lock.unlock()
        if allArrived {
            downstream.receive(subscription: self)
        }
    }

    func receivedChildValue(
        child: any ZipChild,
        store: () -> Void
    ) -> Subscribers.Demand {
        lock.lock()
        store()
        defer {
            checkShouldFinish()
            lock.unlock()
        }
        guard hasCompleteValueAvailable else { return .none }
        value = dequeueValue()
        processingValueForChild = child
        valueIsBeingProcessed = true
        return processValue() ?? .none
    }

    func receivedCompletion(
        _ completion: Subscribers.Completion<Downstream.Failure>,
        forChild child: any ZipChild
    ) {
        switch completion {
        case .failure:
            downstream.receive(completion: completion)
            lock.lock()
            child.state = .failed
            let all = upstreamSubscriptions
            lock.unlock()
            all.forEach { $0.cancel() }
        case .finished:
            lock.lock()
            child.state = .finished
            if !valueIsBeingProcessed {
                valueIsBeingProcessed = true
                if processingValueForChild == nil && !areMoreValuesPossible && !isFinished {
                    sendFinishDownstream()
                } else {
                    processValue()
                }
            }
            lock.unlock()
        }
    }

    private func checkShouldFinish() {
        if processingValueForChild == nil && upstreamSubscriptions.zipShouldFinish() {
            sendFinishDownstream()
            isFinished = true
        }
    }

    private var hasCompleteValueAvailable: Bool {
        upstreamSubscriptions.allSatisfy { $0.hasValue }
    }

    private var areMoreValuesPossible: Bool {
        upstreamSubscriptions.allSatisfy { $0.state == .active || $0.hasValue }
    }

    @discardableResult
    private func processValue() -> Subscribers.Demand? {
        // Called with `lock` held; releases it on exit.
        let deliverable = value
        let newDemandFromCallback: Subscribers.Demand?
        if let deliverable {
            if downstreamDemand != .none {
                downstreamDemand -= 1
            }
            let newDemand = downstream.receive(deliverable)
            newDemandFromCallback = newDemand != .none ? newDemand : nil
            if let newDemandFromCallback {
                downstreamDemand += newDemandFromCallback
                demandReceivedWhileProcessing = newDemandFromCallback
            }
            value = nil
        } else {
            newDemandFromCallback = nil
        }
        let result = demandReceivedWhileProcessing
        valueIsBeingProcessed = false
        processingValueForChild = nil
        demandReceivedWhileProcessing = nil
        return result
    }


    private func sendFinishDownstream() {
        downstream.receive(completion: .finished)
        lock.lock()
        let active = upstreamSubscriptions.filter { $0.state == .active }
        lock.unlock()
        active.forEach { $0.cancel() }
    }

    // MARK: Subscription

    final func request(_ demand: Subscribers.Demand) {
        assert(demand != .none, "request(_:) demand must not be .none")
        lock.lock()
        downstreamDemand += demand
        // Forward to every child except the one whose delivery is in flight.
        // (Targets are computed and requested while the lock stays held — the
        // recursive lock makes synchronous re-entry safe and this matches the
        // reference's effective ordering without a second lock round-trip.)
        let processingIndex = processingValueForChild?.childIndex
        let targets = upstreamSubscriptions.filter { $0.childIndex != processingIndex }
        targets.forEach { $0.request(demand) }
        if valueIsBeingProcessed {
            demandReceivedWhileProcessing = demand
        } else {
            valueIsBeingProcessed = true
            processValue()
        }
        lock.unlock()
    }

    final func cancel() {
        lock.lock()
        let all = upstreamSubscriptions
        lock.unlock()
        all.forEach { $0.cancel() }
    }

    var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
    var playgroundDescription: Any { description }
}

private extension Array where Element == any ZipChild {
    func zipShouldFinish() -> Bool {
        contains { $0.state == .finished && !$0.hasValue }
    }
}

// MARK: - Arity inners

extension Publishers.Zip {
    private final class Inner<Downstream: Subscriber>: ZipCore<Downstream>
    where Downstream.Input == (UpstreamA.Output, UpstreamB.Output),
          Downstream.Failure == UpstreamA.Failure {
        private var childA: ZipChildSubscriber<UpstreamA, Downstream>!
        private var childB: ZipChildSubscriber<UpstreamB, Downstream>!

        init(downstream: Downstream, _ a: UpstreamA, _ b: UpstreamB) {
            super.init(downstream: downstream)
            childA = ZipChildSubscriber(self, 0)
            childB = ZipChildSubscriber(self, 1)
            cachedChildren = [childA, childB]
            a.subscribe(childA)
            b.subscribe(childB)
        }



        override func dequeueValue() -> Downstream.Input {
            (childA.takeValue(),
             childB.takeValue())
        }
    }
}

extension Publishers.Zip3 {
    private final class Inner<Downstream: Subscriber>: ZipCore<Downstream>
    where Downstream.Input == (A.Output, B.Output, C.Output),
          Downstream.Failure == A.Failure {
        private var childA: ZipChildSubscriber<A, Downstream>!
        private var childB: ZipChildSubscriber<B, Downstream>!
        private var childC: ZipChildSubscriber<C, Downstream>!

        init(downstream: Downstream, _ a: A, _ b: B, _ c: C) {
            super.init(downstream: downstream)
            childA = ZipChildSubscriber(self, 0)
            childB = ZipChildSubscriber(self, 1)
            childC = ZipChildSubscriber(self, 2)
            cachedChildren = [childA, childB, childC]
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
        }


        override func dequeueValue() -> Downstream.Input {
            (childA.takeValue(),
             childB.takeValue(),
             childC.takeValue())
        }
    }
}

extension Publishers.Zip4 {
    private final class Inner<Downstream: Subscriber>: ZipCore<Downstream>
    where Downstream.Input == (A.Output, B.Output, C.Output, D.Output),
          Downstream.Failure == A.Failure {
        private var childA: ZipChildSubscriber<A, Downstream>!
        private var childB: ZipChildSubscriber<B, Downstream>!
        private var childC: ZipChildSubscriber<C, Downstream>!
        private var childD: ZipChildSubscriber<D, Downstream>!

        init(downstream: Downstream, _ a: A, _ b: B, _ c: C, _ d: D) {
            super.init(downstream: downstream)
            childA = ZipChildSubscriber(self, 0)
            childB = ZipChildSubscriber(self, 1)
            childC = ZipChildSubscriber(self, 2)
            childD = ZipChildSubscriber(self, 3)
            cachedChildren = [childA, childB, childC, childD]
            a.subscribe(childA)
            b.subscribe(childB)
            c.subscribe(childC)
            d.subscribe(childD)
        }


        override func dequeueValue() -> Downstream.Input {
            (childA.takeValue(),
             childB.takeValue(),
             childC.takeValue(),
             childD.takeValue())
        }
    }
}

// MARK: - zip methods

extension Publisher {
    /// Delivers pairs of elements from this and the other publisher.
    public func zip<Other: Publisher>(_ other: Other) -> Publishers.Zip<Self, Other>
    where Self.Failure == Other.Failure {
        .init(self, other)
    }

    /// Delivers transformed pairs of elements.
    public func zip<Other: Publisher, Result>(
        _ other: Other,
        _ transform: @escaping (Output, Other.Output) -> Result
    ) -> Publishers.Map<Publishers.Zip<Self, Other>, Result>
    where Self.Failure == Other.Failure {
        .init(upstream: .init(self, other), transform: transform)
    }

    /// Delivers 3-tuples from this and two other publishers.
    public func zip<Other1: Publisher, Other2: Publisher>(
        _ publisher1: Other1,
        _ publisher2: Other2
    ) -> Publishers.Zip3<Self, Other1, Other2>
    where Self.Failure == Other1.Failure, Other1.Failure == Other2.Failure {
        .init(self, publisher1, publisher2)
    }

    /// Delivers transformed 3-tuples.
    public func zip<Other1: Publisher, Other2: Publisher, Result>(
        _ publisher1: Other1,
        _ publisher2: Other2,
        _ transform: @escaping (Output, Other1.Output, Other2.Output) -> Result
    ) -> Publishers.Map<Publishers.Zip3<Self, Other1, Other2>, Result>
    where Self.Failure == Other1.Failure, Other1.Failure == Other2.Failure {
        .init(upstream: .init(self, publisher1, publisher2), transform: transform)
    }

    /// Delivers 4-tuples from this and three other publishers.
    public func zip<Other1: Publisher, Other2: Publisher, Other3: Publisher>(
        _ publisher1: Other1,
        _ publisher2: Other2,
        _ publisher3: Other3
    ) -> Publishers.Zip4<Self, Other1, Other2, Other3>
    where Self.Failure == Other1.Failure,
          Other1.Failure == Other2.Failure,
          Other2.Failure == Other3.Failure {
        .init(self, publisher1, publisher2, publisher3)
    }

    /// Delivers transformed 4-tuples.
    public func zip<Other1: Publisher, Other2: Publisher, Other3: Publisher, Result>(
        _ publisher1: Other1,
        _ publisher2: Other2,
        _ publisher3: Other3,
        _ transform: @escaping (Output, Other1.Output, Other2.Output, Other3.Output) -> Result
    ) -> Publishers.Map<Publishers.Zip4<Self, Other1, Other2, Other3>, Result>
    where Self.Failure == Other1.Failure,
          Other1.Failure == Other2.Failure,
          Other2.Failure == Other3.Failure {
        .init(upstream: .init(self, publisher1, publisher2, publisher3), transform: transform)
    }
}
