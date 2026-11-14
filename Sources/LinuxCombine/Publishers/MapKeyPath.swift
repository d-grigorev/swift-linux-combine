extension Publishers {

    /// A publisher that publishes the value of a key path.
    ///
    /// Official API: `struct MapKeyPath<Upstream, Output>` with
    /// `init(upstream:keyPath:)`.
    ///
    /// Semantics (verified by the parity suite): pure pass-through
    /// subscriber (the downstream receives the upstream subscription);
    /// descriptions are "ValueForKey" / "ValueForKeys".
    public struct MapKeyPath<Upstream: Publisher, Output>: Publisher {
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The key path of a property to publish.
        public let keyPath: KeyPath<Upstream.Output, Output>

        public init(upstream: Upstream, keyPath: KeyPath<Upstream.Output, Output>) {
            self.upstream = upstream
            self.keyPath = keyPath
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, parent: self))
        }
    }

    /// A publisher that publishes the values of two key paths as a tuple.
    ///
    /// Official API: `struct MapKeyPath2<Upstream, Output0, Output1>` with
    /// `init(upstream:keyPath0:keyPath1:)`.
    public struct MapKeyPath2<Upstream: Publisher, Output0, Output1>: Publisher {
        public typealias Output = (Output0, Output1)
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The key path of a property to publish.
        public let keyPath0: KeyPath<Upstream.Output, Output0>

        /// The key path of a second property to publish.
        public let keyPath1: KeyPath<Upstream.Output, Output1>

        public init(
            upstream: Upstream,
            keyPath0: KeyPath<Upstream.Output, Output0>,
            keyPath1: KeyPath<Upstream.Output, Output1>
        ) {
            self.upstream = upstream
            self.keyPath0 = keyPath0
            self.keyPath1 = keyPath1
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, parent: self))
        }
    }

    /// A publisher that publishes the values of three key paths as a tuple.
    ///
    /// Official API: `struct MapKeyPath3<Upstream, Output0, Output1, Output2>`
    /// with `init(upstream:keyPath0:keyPath1:keyPath2:)`.
    public struct MapKeyPath3<Upstream: Publisher, Output0, Output1, Output2>: Publisher {
        public typealias Output = (Output0, Output1, Output2)
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The key path of a property to publish.
        public let keyPath0: KeyPath<Upstream.Output, Output0>

        /// The key path of a second property to publish.
        public let keyPath1: KeyPath<Upstream.Output, Output1>

        /// The key path of a third property to publish.
        public let keyPath2: KeyPath<Upstream.Output, Output2>

        public init(
            upstream: Upstream,
            keyPath0: KeyPath<Upstream.Output, Output0>,
            keyPath1: KeyPath<Upstream.Output, Output1>,
            keyPath2: KeyPath<Upstream.Output, Output2>
        ) {
            self.upstream = upstream
            self.keyPath0 = keyPath0
            self.keyPath1 = keyPath1
            self.keyPath2 = keyPath2
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, parent: self))
        }
    }
}

extension Publisher {
    /// Publishes the value of a key path.
    public func map<T>(
        _ keyPath: KeyPath<Output, T>
    ) -> Publishers.MapKeyPath<Self, T> {
        .init(upstream: self, keyPath: keyPath)
    }

    /// Publishes the values of two key paths as a tuple.
    public func map<T0, T1>(
        _ keyPath0: KeyPath<Output, T0>,
        _ keyPath1: KeyPath<Output, T1>
    ) -> Publishers.MapKeyPath2<Self, T0, T1> {
        .init(upstream: self, keyPath0: keyPath0, keyPath1: keyPath1)
    }

    /// Publishes the values of three key paths as a tuple.
    public func map<T0, T1, T2>(
        _ keyPath0: KeyPath<Output, T0>,
        _ keyPath1: KeyPath<Output, T1>,
        _ keyPath2: KeyPath<Output, T2>
    ) -> Publishers.MapKeyPath3<Self, T0, T1, T2> {
        .init(upstream: self, keyPath0: keyPath0, keyPath1: keyPath1, keyPath2: keyPath2)
    }
}

extension Publishers.MapKeyPath {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let keyPath: KeyPath<Input, Output>

        let combineIdentifier = CombineIdentifier()

        fileprivate init(
            downstream: Downstream,
            parent: Publishers.MapKeyPath<Upstream, Output>
        ) {
            self.downstream = downstream
            self.keyPath = parent.keyPath
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            downstream.receive(input[keyPath: keyPath])
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "ValueForKey" }

        var customMirror: Mirror {
            let children: [Mirror.Child] = [("keyPath", keyPath)]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publishers.MapKeyPath2 {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let keyPath0: KeyPath<Input, Output0>
        private let keyPath1: KeyPath<Input, Output1>

        let combineIdentifier = CombineIdentifier()

        fileprivate init(
            downstream: Downstream,
            parent: Publishers.MapKeyPath2<Upstream, Output0, Output1>
        ) {
            self.downstream = downstream
            self.keyPath0 = parent.keyPath0
            self.keyPath1 = parent.keyPath1
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            downstream.receive((input[keyPath: keyPath0], input[keyPath: keyPath1]))
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "ValueForKeys" }

        var customMirror: Mirror {
            let children: [Mirror.Child] = [
                ("keyPath0", keyPath0),
                ("keyPath1", keyPath1),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publishers.MapKeyPath3 {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let keyPath0: KeyPath<Input, Output0>
        private let keyPath1: KeyPath<Input, Output1>
        private let keyPath2: KeyPath<Input, Output2>

        let combineIdentifier = CombineIdentifier()

        fileprivate init(
            downstream: Downstream,
            parent: Publishers.MapKeyPath3<Upstream, Output0, Output1, Output2>
        ) {
            self.downstream = downstream
            self.keyPath0 = parent.keyPath0
            self.keyPath1 = parent.keyPath1
            self.keyPath2 = parent.keyPath2
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            downstream.receive((
                input[keyPath: keyPath0],
                input[keyPath: keyPath1],
                input[keyPath: keyPath2]
            ))
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        var description: String { "ValueForKeys" }

        var customMirror: Mirror {
            let children: [Mirror.Child] = [
                ("keyPath0", keyPath0),
                ("keyPath1", keyPath1),
                ("keyPath2", keyPath2),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
