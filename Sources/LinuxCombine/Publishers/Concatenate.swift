extension Publishers {

    /// A publisher that emits all of one publisher's elements before those
    /// from another publisher.
    ///
    /// Official API: `struct Concatenate<Prefix, Suffix>`.
    ///
    /// Semantics (verified by the parity suite): the downstream subscription
    /// is granted when the prefix subscription arrives; prefix values are
    /// gated by outstanding demand (`pending`); the suffix is subscribed when
    /// the prefix finishes, inheriting the remaining demand; a prefix failure
    /// terminates; suffix completion terminates.
    public struct Concatenate<Prefix: Publisher, Suffix: Publisher>: Publisher
    where Prefix.Failure == Suffix.Failure, Prefix.Output == Suffix.Output {
        public typealias Output = Suffix.Output
        public typealias Failure = Suffix.Failure

        /// The publisher republished in its entirety first.
        public let prefix: Prefix

        /// The publisher republished only after `prefix` finishes.
        public let suffix: Suffix

        public init(prefix: Prefix, suffix: Suffix) {
            self.prefix = prefix
            self.suffix = suffix
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber, suffix: suffix)
            prefix.subscribe(PrefixSubscriber(inner: inner))
        }
    }
}

extension Publishers.Concatenate {

    fileprivate final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Suffix.Output, Downstream.Failure == Suffix.Failure {
        typealias Input = Suffix.Output
        typealias Failure = Suffix.Failure

        private let downstream: Downstream
        private var prefixState = SubscriptionStatus.awaitingSubscription
        private var suffixState = SubscriptionStatus.awaitingSubscription
        private var suffix: Suffix?
        private var pending = Subscribers.Demand.none
        private let lock = Lock()

        fileprivate init(downstream: Downstream, suffix: Suffix) {
            self.downstream = downstream
            self.suffix = suffix
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            pending += demand
            guard let subscription = prefixState.subscription ?? suffixState.subscription else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            let upstream = prefixState.subscription ?? suffixState.subscription
            prefixState = .terminal
            suffixState = .terminal
            let suffix = self.suffix
            self.suffix = nil
            lock.unlock()
            // Deallocating the suffix may run arbitrary user code.
            withExtendedLifetime(suffix) {
                upstream?.cancel()
            }
        }

        var description: String { "Concatenate" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }

        // MARK: Prefix side

        func prefixReceive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = prefixState else {
                lock.unlock()
                subscription.cancel()
                return
            }
            prefixState = .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func prefixReceive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = prefixState, pending != .none else {
                lock.unlock()
                return .none
            }
            pending -= 1
            lock.unlock()
            let newDemand = downstream.receive(input)
            guard newDemand != .none else { return .none }
            lock.lock()
            pending += newDemand
            lock.unlock()
            return newDemand
        }

        func prefixReceive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = prefixState else {
                lock.unlock()
                return
            }
            prefixState = .terminal
            let suffix = self.suffix
            lock.unlock()
            switch completion {
            case .finished:
                suffix?.subscribe(SuffixSubscriber(inner: self))
            case .failure:
                downstream.receive(completion: completion)
            }
        }

        // MARK: Suffix side

        func suffixReceive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = suffixState else {
                lock.unlock()
                subscription.cancel()
                return
            }
            suffixState = .subscribed(subscription)
            let pending = self.pending
            lock.unlock()
            if pending != .none {
                subscription.request(pending)
            }
        }

        func suffixReceive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = suffixState else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            return downstream.receive(input)
        }

        func suffixReceive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = suffixState else {
                lock.unlock()
                return
            }
            prefixState = .terminal
            suffixState = .terminal
            lock.unlock()
            downstream.receive(completion: completion)
        }
    }

    /// Upstream-facing subscriber for the prefix; forwards the inner's identity.
    fileprivate struct PrefixSubscriber<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Suffix.Output, Downstream.Failure == Suffix.Failure {
        typealias Input = Suffix.Output
        typealias Failure = Suffix.Failure

        let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) {
            inner.prefixReceive(subscription: subscription)
        }
        func receive(_ input: Input) -> Subscribers.Demand { inner.prefixReceive(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            inner.prefixReceive(completion: completion)
        }
        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }

    /// Replacement-facing subscriber for the suffix; forwards the inner's identity.
    fileprivate struct SuffixSubscriber<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Suffix.Output, Downstream.Failure == Suffix.Failure {
        typealias Input = Suffix.Output
        typealias Failure = Suffix.Failure

        let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) {
            inner.suffixReceive(subscription: subscription)
        }
        func receive(_ input: Input) -> Subscribers.Demand { inner.suffixReceive(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            inner.suffixReceive(completion: completion)
        }
        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }
}

// MARK: - append / prepend methods (official overload set)

extension Publisher {
    /// Appends the given elements after this publisher's elements.
    public func append(
        _ elements: Output...
    ) -> Publishers.Concatenate<Self, Publishers.Sequence<[Output], Failure>> {
        append(elements)
    }

    /// Appends the elements of a sequence after this publisher's elements.
    public func append<S: Sequence>(
        _ elements: S
    ) -> Publishers.Concatenate<Self, Publishers.Sequence<S, Failure>>
    where Output == S.Element {
        .init(prefix: self, suffix: .init(sequence: elements))
    }

    /// Appends another publisher after this one finishes.
    public func append<P: Publisher>(
        _ publisher: P
    ) -> Publishers.Concatenate<Self, P>
    where P.Failure == Failure, P.Output == Output {
        .init(prefix: self, suffix: publisher)
    }

    /// Prepends the given elements before this publisher's elements.
    public func prepend(
        _ elements: Output...
    ) -> Publishers.Concatenate<Publishers.Sequence<[Output], Failure>, Self> {
        prepend(elements)
    }

    /// Prepends the elements of a sequence before this publisher's elements.
    public func prepend<S: Sequence>(
        _ elements: S
    ) -> Publishers.Concatenate<Publishers.Sequence<S, Failure>, Self>
    where Output == S.Element {
        .init(prefix: .init(sequence: elements), suffix: self)
    }

    /// Prepends another publisher before this one.
    public func prepend<P: Publisher>(
        _ publisher: P
    ) -> Publishers.Concatenate<P, Self>
    where P.Failure == Failure, P.Output == Output {
        .init(prefix: publisher, suffix: self)
    }
}
