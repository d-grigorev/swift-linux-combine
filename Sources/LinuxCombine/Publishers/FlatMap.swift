extension Publishers {

    /// A publisher that transforms elements from an upstream publisher into
    /// a new publisher, with bounded concurrency.
    ///
    /// Official API: `struct FlatMap<Child, Upstream>` with
    /// `Child.Failure == Upstream.Failure`.
    ///
    /// Semantics (verified by the parity suite): the outer subscription
    /// requests `maxPublishers` up front (and `.max(1)` more per inner
    /// completion when bounded); inner subscriptions start with `.max(1)`
    /// outstanding demand (or `.unlimited` when the downstream demand is
    /// unlimited); values arriving with no downstream demand are buffered and
    /// drained on `request(_:)` (each drain replenishing the inner by one);
    /// inner failures cancel siblings and terminate; the stream finishes when
    /// the outer publisher has finished and no buffer/inner remains.
    public struct FlatMap<Child: Publisher, Upstream: Publisher>: Publisher
    where Child.Failure == Upstream.Failure {
        public typealias Output = Child.Output
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let maxPublishers: Subscribers.Demand
        public let transform: (Upstream.Output) -> Child

        public init(
            upstream: Upstream,
            maxPublishers: Subscribers.Demand,
            transform: @escaping (Upstream.Output) -> Child
        ) {
            self.upstream = upstream
            self.maxPublishers = maxPublishers
            self.transform = transform
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let outer = Outer(downstream: subscriber, maxPublishers: maxPublishers, map: transform)
            subscriber.receive(subscription: outer)
            upstream.subscribe(outer)
        }
    }
}

extension Publishers.FlatMap {

    private final class Outer<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Child.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let outerLock = RecursiveLock()
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()

        private let downstream: Downstream
        private var downstreamDemand = Subscribers.Demand.none
        private var downstreamRecursive = false
        private var innerRecursive = false
        private var subscriptions: [Int: any Subscription] = [:]
        private var nextInnerIndex = 0
        private var pendingSubscriptions = 0
        private var buffer: [(Int, Child.Output)] = []
        private let maxPublishers: Subscribers.Demand
        private let map: (Input) -> Child
        private var cancelledOrCompleted = false
        private var outerFinished = false
        private var outerSubscription: (any Subscription)?

        init(
            downstream: Downstream,
            maxPublishers: Subscribers.Demand,
            map: @escaping (Input) -> Child
        ) {
            self.downstream = downstream
            self.maxPublishers = maxPublishers
            self.map = map
        }

        // MARK: Subscriber

        func receive(subscription: any Subscription) {
            lock.lock()
            guard outerSubscription == nil, !cancelledOrCompleted else {
                lock.unlock()
                subscription.cancel()
                return
            }
            outerSubscription = subscription
            lock.unlock()
            subscription.request(maxPublishers)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            let cancelled = cancelledOrCompleted
            lock.unlock()
            guard !cancelled else { return .none }

            let child = map(input)   // user closure: never under lock

            lock.lock()
            let innerIndex = nextInnerIndex
            nextInnerIndex += 1
            pendingSubscriptions += 1
            lock.unlock()
            child.subscribe(Side(index: innerIndex, inner: self))
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            outerSubscription = nil
            outerFinished = true
            switch completion {
            case .finished:
                releaseLockThenSendCompletionDownstreamIfNeeded(outerFinished: true)
                return
            case .failure:
                let wasAlreadyCancelled = cancelledOrCompleted
                cancelledOrCompleted = true
                let subscriptions = self.subscriptions
                self.subscriptions = [:]
                // Cancelling with the lock acquired mirrors Combine (tested).
                for (_, subscription) in subscriptions {
                    subscription.cancel()
                }
                lock.unlock()
                guard !wasAlreadyCancelled else { return }
                downstreamLock.lock()
                downstream.receive(completion: completion)
                downstreamLock.unlock()
            }
        }

        // MARK: Subscription

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            if downstreamRecursive {
                // downstreamLock is already held by this task (re-entrant
                // request from a delivery callback): just accumulate.
                downstreamDemand += demand
                return
            }
            lock.lock()
            if cancelledOrCompleted {
                lock.unlock()
                return
            }
            if demand == .unlimited {
                downstreamDemand = .unlimited
                let buffer = self.buffer
                self.buffer = []
                let subscriptions = self.subscriptions
                lock.unlock()
                downstreamLock.lock()
                downstreamRecursive = true
                for (_, value) in buffer {
                    _ = downstream.receive(value)
                }
                downstreamRecursive = false
                downstreamLock.unlock()
                for (_, subscription) in subscriptions {
                    subscription.request(.unlimited)
                }
                lock.lock()
            } else {
                downstreamDemand += demand
                while !buffer.isEmpty, downstreamDemand > 0 {
                    let (index, value) = buffer.removeFirst()
                    downstreamDemand -= 1
                    let subscription = subscriptions[index]
                    lock.unlock()
                    downstreamLock.lock()
                    downstreamRecursive = true
                    let additionalDemand = downstream.receive(value)
                    downstreamRecursive = false
                    downstreamLock.unlock()
                    if additionalDemand != .none {
                        lock.lock()
                        downstreamDemand += additionalDemand
                        lock.unlock()
                    }
                    if let subscription {
                        innerRecursive = true
                        subscription.request(.max(1))
                        innerRecursive = false
                    }
                    lock.lock()
                }
            }
            releaseLockThenSendCompletionDownstreamIfNeeded(outerFinished: outerFinished)
        }

        func cancel() {
            lock.lock()
            guard !cancelledOrCompleted else {
                lock.unlock()
                return
            }
            cancelledOrCompleted = true
            let subscriptions = self.subscriptions
            self.subscriptions = [:]
            let outerSubscription = self.outerSubscription
            self.outerSubscription = nil
            lock.unlock()
            for (_, subscription) in subscriptions {
                subscription.cancel()
            }
            outerSubscription?.cancel()
        }

        var description: String { "FlatMap" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }

        // MARK: Inner (Side) coordination

        func receiveInner(subscription: any Subscription, _ index: Int) {
            lock.lock()
            pendingSubscriptions -= 1
            subscriptions[index] = subscription
            let demand: Subscribers.Demand = downstreamDemand == .unlimited ? .unlimited : .max(1)
            lock.unlock()
            subscription.request(demand)
        }

        func receiveInner(_ input: Child.Output, _ index: Int) -> Subscribers.Demand {
            lock.lock()
            if downstreamDemand == .unlimited {
                lock.unlock()
                downstreamLock.lock()
                downstreamRecursive = true
                _ = downstream.receive(input)
                downstreamRecursive = false
                downstreamLock.unlock()
                return .none
            }
            if downstreamDemand == .none || innerRecursive {
                buffer.append((index, input))
                lock.unlock()
                return .none
            }
            downstreamDemand -= 1
            lock.unlock()
            downstreamLock.lock()
            downstreamRecursive = true
            let newDemand = downstream.receive(input)
            downstreamRecursive = false
            downstreamLock.unlock()
            if newDemand > 0 {
                lock.lock()
                downstreamDemand += newDemand
                lock.unlock()
            }
            return .max(1)
        }

        func receiveInner(completion: Subscribers.Completion<Child.Failure>, _ index: Int) {
            switch completion {
            case .finished:
                lock.lock()
                subscriptions.removeValue(forKey: index)
                let downstreamCompleted = releaseLockThenSendCompletionDownstreamIfNeeded(
                    outerFinished: outerFinished
                )
                if !downstreamCompleted {
                    requestOneMorePublisher()
                }
            case .failure:
                lock.lock()
                if cancelledOrCompleted {
                    lock.unlock()
                    return
                }
                cancelledOrCompleted = true
                let subscriptions = self.subscriptions
                self.subscriptions = [:]
                lock.unlock()
                for (i, subscription) in subscriptions where i != index {
                    subscription.cancel()
                }
                downstreamLock.lock()
                downstream.receive(completion: completion)
                downstreamLock.unlock()
            }
        }

        private func requestOneMorePublisher() {
            guard maxPublishers != .unlimited else { return }
            outerLock.lock()
            outerSubscription?.request(.max(1))
            outerLock.unlock()
        }

        /// Lock held on entry; released before returning (true = completion sent).
        @discardableResult
        private func releaseLockThenSendCompletionDownstreamIfNeeded(outerFinished: Bool) -> Bool {
            if !cancelledOrCompleted, outerFinished, buffer.isEmpty,
               subscriptions.count + pendingSubscriptions == 0 {
                cancelledOrCompleted = true
                lock.unlock()
                downstreamLock.lock()
                downstream.receive(completion: .finished)
                downstreamLock.unlock()
                return true
            }
            lock.unlock()
            return false
        }

        // MARK: Side

        fileprivate struct Side: Subscriber,
            CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
        {
            typealias Input = Child.Output
            typealias Failure = Child.Failure

            private let index: Int
            private let inner: Outer
            fileprivate let combineIdentifier = CombineIdentifier()

            fileprivate init(index: Int, inner: Outer) {
                self.index = index
                self.inner = inner
            }

            fileprivate func receive(subscription: any Subscription) {
                inner.receiveInner(subscription: subscription, index)
            }

            fileprivate func receive(_ input: Child.Output) -> Subscribers.Demand {
                inner.receiveInner(input, index)
            }

            fileprivate func receive(completion: Subscribers.Completion<Child.Failure>) {
                inner.receiveInner(completion: completion, index)
            }

            fileprivate var description: String { "FlatMap" }
            fileprivate var customMirror: Mirror {
                Mirror(
                    self,
                    children: CollectionOfOne<Mirror.Child>(
                        ("parentSubscription", inner.combineIdentifier)
                    )
                )
            }
            fileprivate var playgroundDescription: Any { description }
        }
    }
}

extension Publisher {
    /// Transforms each element into a publisher, flattening the results.
    public func flatMap<Result, Child: Publisher>(
        maxPublishers: Subscribers.Demand = .unlimited,
        _ transform: @escaping (Output) -> Child
    ) -> Publishers.FlatMap<Child, Self>
    where Result == Child.Output, Failure == Child.Failure {
        .init(upstream: self, maxPublishers: maxPublishers, transform: transform)
    }

    /// Flattening variant for non-failing children of a non-failing upstream.
    public func flatMap<Child: Publisher>(
        maxPublishers: Subscribers.Demand = .unlimited,
        _ transform: @escaping (Output) -> Child
    ) -> Publishers.FlatMap<Child, Publishers.SetFailureType<Self, Child.Failure>>
    where Failure == Never {
        setFailureType(to: Child.Failure.self)
            .flatMap(maxPublishers: maxPublishers, transform)
    }

    /// Flattening variant for non-failing upstream and children.
    public func flatMap<Child: Publisher>(
        maxPublishers: Subscribers.Demand = .unlimited,
        _ transform: @escaping (Output) -> Child
    ) -> Publishers.FlatMap<Child, Self>
    where Failure == Never, Child.Failure == Never {
        .init(upstream: self, maxPublishers: maxPublishers, transform: transform)
    }

    /// Flattening variant for a non-failing upstream with failing children.
    public func flatMap<Child: Publisher>(
        maxPublishers: Subscribers.Demand = .unlimited,
        _ transform: @escaping (Output) -> Child
    ) -> Publishers.FlatMap<Publishers.SetFailureType<Child, Failure>, Self>
    where Child.Failure == Never {
        flatMap(maxPublishers: maxPublishers) {
            transform($0).setFailureType(to: Failure.self)
        }
    }
}
