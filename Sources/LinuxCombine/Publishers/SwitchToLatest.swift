extension Publishers {

    /// A publisher that flattens nested publishers, republishing elements from
    /// only the most recent one.
    ///
    /// Official API: `struct SwitchToLatest<NestedPublisher, Upstream>`.
    ///
    /// Semantics (verified by the parity suite): the outer subscription is
    /// requested with `.unlimited`; each new inner publisher cancels the
    /// previous one; only the current inner's elements are delivered (indexed
    /// staleness guards); downstream demand is tracked and forwarded to each
    /// new inner on arrival; an inner element with no outstanding demand
    /// saturates (Apple behavior); upstream finish completes downstream only
    /// after the current inner finishes; failures terminate immediately,
    /// cancelling the other side.
    public struct SwitchToLatest<NestedPublisher: Publisher, Upstream: Publisher>: Publisher
    where Upstream.Output == NestedPublisher, Upstream.Failure == NestedPublisher.Failure {
        public typealias Output = NestedPublisher.Output
        public typealias Failure = NestedPublisher.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let outer = Outer(downstream: subscriber)
            subscriber.receive(subscription: outer)
            upstream.subscribe(outer)
        }
    }
}

extension Publishers.SwitchToLatest {

    private final class Outer<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == NestedPublisher.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private var outerSubscription: (any Subscription)?
        private var currentInnerSubscription: (any Subscription)?
        private var currentInnerIndex: UInt64 = 0
        private var nextInnerIndex: UInt64 = 1
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()
        private var cancelled = false
        private var finished = false
        private var sentCompletion = false
        private var awaitingInnerSubscription = false
        private var downstreamDemand = Subscribers.Demand.none

        init(downstream: Downstream) {
            self.downstream = downstream
        }

        // MARK: Subscriber (outer)

        func receive(subscription: any Subscription) {
            lock.lock()
            guard outerSubscription == nil, !cancelled else {
                lock.unlock()
                subscription.cancel()
                return
            }
            outerSubscription = subscription
            lock.unlock()
            subscription.request(.unlimited)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if cancelled || finished {
                lock.unlock()
                return .none
            }
            let previousInner = currentInnerSubscription
            currentInnerSubscription = nil
            let index = nextInnerIndex
            currentInnerIndex = index
            nextInnerIndex += 1
            awaitingInnerSubscription = true
            lock.unlock()
            previousInner?.cancel()
            input.subscribe(Side(inner: self, index: index))
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            outerSubscription = nil
            finished = true
            if cancelled {
                lock.unlock()
                return
            }
            switch completion {
            case .finished:
                if awaitingInnerSubscription {
                    lock.unlock()
                    return
                }
                if currentInnerSubscription == nil {
                    sentCompletion = true
                    lock.unlock()
                    downstreamLock.lock()
                    downstream.receive(completion: completion)
                    downstreamLock.unlock()
                } else {
                    lock.unlock()
                }
            case .failure:
                let previousInner = currentInnerSubscription
                currentInnerSubscription = nil
                sentCompletion = true
                lock.unlock()
                previousInner?.cancel()
                downstreamLock.lock()
                downstream.receive(completion: completion)
                downstreamLock.unlock()
            }
        }

        // MARK: Subscription

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            downstreamDemand += demand
            if let currentInnerSubscription {
                lock.unlock()
                currentInnerSubscription.request(demand)
            } else {
                lock.unlock()
            }
        }

        func cancel() {
            lock.lock()
            cancelled = true
            let inner = currentInnerSubscription
            currentInnerSubscription = nil
            let outer = outerSubscription
            outerSubscription = nil
            lock.unlock()
            inner?.cancel()
            outer?.cancel()
        }

        var description: String { "SwitchToLatest" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }

        // MARK: Inner (Side) coordination

        func receiveInner(subscription: any Subscription, _ index: UInt64) {
            lock.lock()
            guard currentInnerIndex == index, !cancelled, currentInnerSubscription == nil else {
                lock.unlock()
                subscription.cancel()
                return
            }
            currentInnerSubscription = subscription
            awaitingInnerSubscription = false
            let demand = downstreamDemand
            lock.unlock()
            if demand > 0 {
                subscription.request(demand)
            }
        }

        func receiveInner(_ input: NestedPublisher.Output, _ index: UInt64) -> Subscribers.Demand {
            lock.lock()
            guard currentInnerIndex == index, !cancelled else {
                lock.unlock()
                return .none
            }
            // Saturating subtraction mirrors Apple (delivery without demand
            // clamps at zero rather than underflowing).
            downstreamDemand -= 1
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(input)
            downstreamLock.unlock()
            if newDemand > 0 {
                lock.lock()
                downstreamDemand += newDemand
                lock.unlock()
            }
            return newDemand
        }

        func receiveInner(
            completion: Subscribers.Completion<Failure>,
            _ index: UInt64
        ) {
            lock.lock()
            guard currentInnerIndex == index, !cancelled else {
                lock.unlock()
                return
            }
            assert(!awaitingInnerSubscription, "Unexpected completion")
            currentInnerSubscription = nil
            switch completion {
            case .finished:
                if sentCompletion || !finished {
                    lock.unlock()
                    return
                }
                sentCompletion = true
                lock.unlock()
                downstreamLock.lock()
                downstream.receive(completion: completion)
                downstreamLock.unlock()
            case .failure:
                if sentCompletion {
                    lock.unlock()
                    return
                }
                cancelled = true
                let outer = outerSubscription
                outerSubscription = nil
                sentCompletion = true
                lock.unlock()
                outer?.cancel()
                downstreamLock.lock()
                downstream.receive(completion: completion)
                downstreamLock.unlock()
            }
        }

        // MARK: Side

        fileprivate struct Side: Subscriber,
            CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
        {
            typealias Input = NestedPublisher.Output
            typealias Failure = NestedPublisher.Failure

            private let index: UInt64
            private let outer: Outer
            let combineIdentifier = CombineIdentifier()

            fileprivate init(inner: Outer, index: UInt64) {
                self.index = index
                self.outer = inner
            }

            fileprivate func receive(subscription: any Subscription) {
                outer.receiveInner(subscription: subscription, index)
            }

            fileprivate func receive(_ input: Input) -> Subscribers.Demand {
                outer.receiveInner(input, index)
            }

            fileprivate func receive(completion: Subscribers.Completion<Failure>) {
                outer.receiveInner(completion: completion, index)
            }

            fileprivate var description: String { "SwitchToLatest" }
            fileprivate var customMirror: Mirror {
                Mirror(
                    self,
                    children: CollectionOfOne<Mirror.Child>(
                        ("parentSubscription", outer.combineIdentifier)
                    )
                )
            }
            fileprivate var playgroundDescription: Any { description }
        }
    }
}

// MARK: - switchToLatest methods (official overload set)

extension Publisher where Output: Publisher, Output.Failure == Failure {
    /// Republishes elements sent by the most recently received publisher.
    public func switchToLatest() -> Publishers.SwitchToLatest<Output, Self> {
        .init(upstream: self)
    }
}

extension Publisher where Output: Publisher, Failure == Never {
    /// Republishes elements sent by the most recently received publisher.
    public func switchToLatest()
    -> Publishers.SwitchToLatest<Output, Publishers.SetFailureType<Self, Output.Failure>> {
        setFailureType(to: Output.Failure.self).switchToLatest()
    }
}

extension Publisher where Output: Publisher, Failure == Never, Output.Failure == Never {
    /// Republishes elements sent by the most recently received publisher.
    public func switchToLatest() -> Publishers.SwitchToLatest<Output, Self> {
        .init(upstream: self)
    }
}

extension Publisher where Output: Publisher, Output.Failure == Never {
    /// Republishes elements sent by the most recently received publisher.
    public func switchToLatest() -> Publishers.SwitchToLatest<
        Publishers.SetFailureType<Output, Failure>,
        Publishers.Map<Self, Publishers.SetFailureType<Output, Failure>>
    > {
        map { $0.setFailureType(to: Failure.self) }.switchToLatest()
    }
}
