extension Publishers {

    /// A publisher that ignores elements from the upstream publisher until
    /// it receives an element from a second publisher.
    ///
    /// Official API: `struct DropUntilOutput<Upstream, Other>` with
    /// `init(upstream:other:)` and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): the downstream receives the
    /// inner subscription immediately (before either upstream is
    /// subscribed); dropped values still count against the accumulated
    /// demand (`pendingDemand -= 1`); the other publisher is asked for one
    /// value; its completion without output cancels this publisher.
    public struct DropUntilOutput<Upstream: Publisher, Other: Publisher>: Publisher
    where Upstream.Failure == Other.Failure {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher that this publisher receives elements from.
        public let upstream: Upstream

        /// A publisher to monitor for its first emitted element.
        public let other: Other

        public init(upstream: Upstream, other: Other) {
            self.upstream = upstream
            self.other = other
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber)
            subscriber.receive(subscription: inner)
            other.subscribe(Inner.OtherSubscriber(inner: inner))
            upstream.subscribe(inner)
        }
    }
}

extension Publisher {
    /// Ignores elements from the upstream publisher until it receives an
    /// element from a second publisher.
    public func drop<Other: Publisher>(
        untilOutputFrom publisher: Other
    ) -> Publishers.DropUntilOutput<Self, Other> where Failure == Other.Failure {
        .init(upstream: self, other: publisher)
    }
}

extension Publishers.DropUntilOutput: Equatable
where Upstream: Equatable, Other: Equatable {}

extension Publishers.DropUntilOutput {

    fileprivate final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstreamLock = RecursiveLock()
        private let downstream: Downstream
        private var triggered = false
        private var upstreamSubscription: (any Subscription)?
        private var pendingDemand = Subscribers.Demand.none
        private var otherSubscription: (any Subscription)?
        private var otherFinished = false
        private var cancelled = false

        init(downstream: Downstream) {
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard upstreamSubscription == nil && !cancelled else {
                lock.unlock()
                subscription.cancel()
                return
            }
            upstreamSubscription = subscription
            if pendingDemand > 0 {
                lock.unlock()
                subscription.request(pendingDemand)
            } else {
                lock.unlock()
            }
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if !triggered || cancelled {
                pendingDemand -= 1
                lock.unlock()
                return .none
            }
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(input)
            downstreamLock.unlock()
            return newDemand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            if cancelled {
                lock.unlock()
                return
            }
            cancelled = true
            lock.unlock()
            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }

        private func receiveOther(subscription: any Subscription) {
            // Apple's implementation doesn't lock here; mirrored.
            guard otherSubscription == nil else {
                subscription.cancel()
                return
            }
            otherSubscription = subscription
            subscription.request(.max(1))
        }

        private func receiveOther(_ input: Other.Output) -> Subscribers.Demand {
            lock.lock()
            triggered = true
            otherSubscription = nil
            lock.unlock()
            return .none
        }

        private func receiveOther(completion: Subscribers.Completion<Other.Failure>) {
            lock.lock()
            if triggered {
                otherSubscription = nil
                lock.unlock()
                return
            }

            otherFinished = true
            let upstreamSubscription = self.upstreamSubscription
            self.upstreamSubscription = nil
            lock.unlock()
            upstreamSubscription?.cancel()
            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            pendingDemand += demand
            if let subscription = upstreamSubscription {
                lock.unlock()
                subscription.request(demand)
            } else {
                lock.unlock()
            }
        }

        func cancel() {
            lock.lock()
            let upstreamSubscription = self.upstreamSubscription
            let otherSubscription = self.otherSubscription
            self.upstreamSubscription = nil
            self.otherSubscription = nil
            cancelled = true
            lock.unlock()

            upstreamSubscription?.cancel()
            otherSubscription?.cancel()
        }

        var description: String { "DropUntilOutput" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }
    }
}

extension Publishers.DropUntilOutput.Inner {

    fileprivate struct OtherSubscriber: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    {
        let inner: Publishers.DropUntilOutput<Upstream, Other>.Inner<Downstream>

        var combineIdentifier: CombineIdentifier {
            inner.combineIdentifier
        }

        func receive(subscription: any Subscription) {
            inner.receiveOther(subscription: subscription)
        }

        func receive(_ input: Other.Output) -> Subscribers.Demand {
            inner.receiveOther(input)
        }

        func receive(completion: Subscribers.Completion<Other.Failure>) {
            inner.receiveOther(completion: completion)
        }

        var description: String { "DropUntilOutput" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }
    }
}
