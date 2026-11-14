extension Publishers {

    /// A publisher that ignores all upstream elements, but passes along a
    /// completion state (finished or failed).
    ///
    /// Official API: `struct IgnoreOutput<Upstream>` with
    /// `init(upstream:)`, conditional `Equatable`, and `Output == Never`.
    ///
    /// Semantics (verified by the parity suite): the downstream receives the
    /// *upstream* subscription directly (the inner is a plain subscriber),
    /// which is then requested `.unlimited`.
    public struct IgnoreOutput<Upstream: Publisher>: Publisher {
        public typealias Output = Never
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Never, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber))
        }
    }
}

extension Publisher {
    /// Ingores all upstream elements, but passes along a completion state.
    public func ignoreOutput() -> Publishers.IgnoreOutput<Self> {
        .init(upstream: self)
    }
}

extension Publishers.IgnoreOutput: Equatable where Upstream: Equatable {}

extension Publishers.IgnoreOutput {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Never, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream

        let combineIdentifier = CombineIdentifier()

        fileprivate init(downstream: Downstream) {
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
            subscription.request(.unlimited)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "IgnoreOutput" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }
    }
}
