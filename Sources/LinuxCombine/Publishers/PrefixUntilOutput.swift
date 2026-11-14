extension Publishers {

    /// A publisher that republishes elements until another publisher emits
    /// an element.
    ///
    /// Official API: `struct PrefixUntilOutput<Upstream, Other>` with
    /// `init(upstream:other:)`.
    ///
    /// Semantics (verified by the parity suite): the trigger publisher is
    /// subscribed during initialization (before the upstream); when the
    /// trigger emits, the upstream subscription is cancelled and the
    /// downstream receives `.finished`; a trigger completion (without
    /// output) terminates the trigger side silently.
    public struct PrefixUntilOutput<Upstream: Publisher, Other: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// Another publisher, whose first output causes this publisher to
        /// finish.
        public let other: Other

        public init(upstream: Upstream, other: Other) {
            self.upstream = upstream
            self.other = other
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, trigger: other))
        }
    }
}

extension Publisher {
    /// Republishes elements until another publisher emits an element.
    public func prefix<Other: Publisher>(
        untilOutputFrom publisher: Other
    ) -> Publishers.PrefixUntilOutput<Self, Other> {
        .init(upstream: self, other: publisher)
    }
}

extension Publishers.PrefixUntilOutput {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private struct Termination: Subscriber {
            let inner: Inner

            var combineIdentifier: CombineIdentifier {
                inner.combineIdentifier
            }

            func receive(subscription: any Subscription) {
                inner.terminationReceive(subscription: subscription)
            }

            func receive(_ input: Other.Output) -> Subscribers.Demand {
                inner.terminationReceive(input)
            }

            func receive(completion: Subscribers.Completion<Other.Failure>) {
                inner.terminationReceive(completion: completion)
            }
        }

        private let lock = Lock()
        private let downstream: Downstream
        private var termination: Termination?
        private var prefixState = SubscriptionStatus.awaitingSubscription
        private var terminationState = SubscriptionStatus.awaitingSubscription
        private var triggered = false

        init(downstream: Downstream, trigger: Other) {
            self.downstream = downstream
            let termination = Termination(inner: self)
            self.termination = termination
            trigger.subscribe(termination)
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = prefixState else {
                lock.unlock()
                subscription.cancel()
                return
            }
            prefixState = triggered ? .terminal : .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = prefixState else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            let prefixState = self.prefixState
            let terminationSubscription = terminationState.subscription
            self.prefixState = .terminal
            terminationState = .terminal
            termination = nil
            lock.unlock()
            terminationSubscription?.cancel()
            if case .subscribed = prefixState {
                downstream.receive(completion: completion)
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = prefixState else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            let prefixSubscription = prefixState.subscription
            let terminationSubscription = terminationState.subscription
            prefixState = .terminal
            terminationState = .terminal
            lock.unlock()
            prefixSubscription?.cancel()
            terminationSubscription?.cancel()
        }

        // MARK: - Trigger side

        private func terminationReceive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = terminationState else {
                lock.unlock()
                subscription.cancel()
                return
            }
            terminationState = .subscribed(subscription)
            lock.unlock()
            subscription.request(.max(1))
        }

        private func terminationReceive(_ input: Other.Output) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = terminationState else {
                lock.unlock()
                return .none
            }
            let prefixSubscription = prefixState.subscription
            prefixState = .terminal
            terminationState = .terminal
            termination = nil
            triggered = true
            lock.unlock()
            prefixSubscription?.cancel()
            downstream.receive(completion: .finished)
            return .none
        }

        private func terminationReceive(
            completion: Subscribers.Completion<Other.Failure>
        ) {
            lock.lock()
            terminationState = .terminal
            termination = nil
            lock.unlock()
        }
    }
}
