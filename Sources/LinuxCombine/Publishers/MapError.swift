extension Publishers {

    /// A publisher that converts any failure from the upstream publisher
    /// into a new error.
    ///
    /// Official API: `struct MapError<Upstream, Failure>`.
    /// Stateless relay: the upstream subscription passes straight through.
    public struct MapError<Upstream: Publisher, Failure: Error>: Publisher {
        public typealias Output = Upstream.Output

        public let upstream: Upstream
        public let transform: (Upstream.Failure) -> Failure

        public init(upstream: Upstream, _ map: @escaping (Upstream.Failure) -> Failure) {
            self.upstream = upstream
            self.transform = map
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, map: transform))
        }
    }

    /// A publisher that provides a failure type for an upstream publisher
    /// that cannot fail.
    ///
    /// Official API: `struct SetFailureType<Upstream, Failure>` where
    /// `Upstream.Failure == Never`.
    public struct SetFailureType<Upstream: Publisher, Failure: Error>: Publisher
    where Upstream.Failure == Never {
        public typealias Output = Upstream.Output

        public let upstream: Upstream

        public init(upstream: Upstream, failureType: Failure.Type) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber))
        }
    }
}

extension Publishers.MapError {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Upstream.Output == Downstream.Input {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let map: (Upstream.Failure) -> Downstream.Failure
        let combineIdentifier = CombineIdentifier()

        fileprivate init(
            downstream: Downstream,
            map: @escaping (Upstream.Failure) -> Downstream.Failure
        ) {
            self.downstream = downstream
            self.map = map
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let error):
                downstream.receive(completion: .failure(map(error)))
            }
        }

        var description: String { "MapError" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publishers.SetFailureType {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Upstream.Output == Downstream.Input, Upstream.Failure == Never {
        typealias Input = Upstream.Output
        typealias Failure = Never

        private let downstream: Downstream
        let combineIdentifier = CombineIdentifier()

        fileprivate init(downstream: Downstream) {
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            // Upstream.Failure == Never: only `.finished` is possible.
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let never):
                switch never {}
            }
        }

        var description: String { "SetFailureType" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Converts any failure from the upstream publisher into a new error.
    public func mapError<E: Error>(_ transform: @escaping (Failure) -> E)
    -> Publishers.MapError<Self, E> {
        .init(upstream: self, transform)
    }

    /// Provides an explicit failure type for a non-failing publisher.
    public func setFailureType<E: Error>(to failureType: E.Type)
    -> Publishers.SetFailureType<Self, E> {
        .init(upstream: self, failureType: failureType)
    }
}
