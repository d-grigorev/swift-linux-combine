extension Publisher where Output: Equatable {

    /// Publishes a Boolean value upon receiving an element equal to the
    /// argument.
    ///
    /// Official API (verified against Apple DocC):
    /// `contains(_ output: Self.Output) -> Publishers.Contains<Self>`.
    public func contains(_ output: Output) -> Publishers.Contains<Self> {
        .init(upstream: self, output: output)
    }
}

extension Publisher {

    /// Publishes a Boolean value upon receiving an element that satisfies
    /// the predicate closure.
    ///
    /// Official API (verified against Apple DocC):
    /// `contains(where:) -> Publishers.ContainsWhere<Self>`.
    public func contains(
        where predicate: @escaping (Output) -> Bool
    ) -> Publishers.ContainsWhere<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Publishes a Boolean value upon receiving an element that satisfies
    /// the throwing predicate closure.
    ///
    /// Official API (verified against Apple DocC):
    /// `tryContains(where:) -> Publishers.TryContainsWhere<Self>`.
    public func tryContains(
        where predicate: @escaping (Output) throws -> Bool
    ) -> Publishers.TryContainsWhere<Self> {
        .init(upstream: self, predicate: predicate)
    }
}

extension Publishers {

    /// A publisher that emits a Boolean value when a specified element is
    /// received from its upstream publisher.
    ///
    /// Official API: `struct Contains<Upstream> where Upstream.Output:
    /// Equatable` with `init(upstream:output:)` and conditional
    /// `Equatable`.
    ///
    /// Semantics (verified by the parity suite): initial result `false`;
    /// a matching element cancels the upstream and emits `true` (held
    /// until the first downstream request); no match before a clean
    /// finish emits `false`.
    public struct Contains<Upstream: Publisher>: Publisher
    where Upstream.Output: Equatable {
        public typealias Output = Bool
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The element to scan for in the upstream publisher.
        public let output: Upstream.Output

        public init(upstream: Upstream, output: Upstream.Output) {
            self.upstream = upstream
            self.output = output
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, output: output))
        }
    }

    /// A publisher that emits a Boolean value upon receiving an element
    /// that satisfies the predicate closure.
    ///
    /// Official API: `struct ContainsWhere<Upstream>` with
    /// `init(upstream:predicate:)`.
    public struct ContainsWhere<Upstream: Publisher>: Publisher {
        public typealias Output = Bool
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The closure that determines whether the publisher should
        /// consider an element as a match.
        public let predicate: (Upstream.Output) -> Bool

        public init(upstream: Upstream, predicate: @escaping (Upstream.Output) -> Bool) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: predicate))
        }
    }

    /// A publisher that emits a Boolean value upon receiving an element
    /// that satisfies the throwing predicate closure.
    ///
    /// Official API: `struct TryContainsWhere<Upstream>` with
    /// `init(upstream:predicate:)`; `Failure == Error`.
    public struct TryContainsWhere<Upstream: Publisher>: Publisher {
        public typealias Output = Bool
        public typealias Failure = Error

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The error-throwing closure that determines whether this
        /// publisher should emit a `true` element.
        public let predicate: (Upstream.Output) throws -> Bool

        public init(
            upstream: Upstream,
            predicate: @escaping (Upstream.Output) throws -> Bool
        ) {
            self.upstream = upstream
            self.predicate = predicate
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, predicate: predicate))
        }
    }
}

extension Publishers.Contains: Equatable where Upstream: Equatable {}

extension Publishers.Contains {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Bool, Upstream.Failure, Void
    > where Downstream.Input == Bool, Downstream.Failure == Upstream.Failure {
        fileprivate init(downstream: Downstream, output: Upstream.Output) {
            self.output = output
            super.init(downstream: downstream, initial: false, reduce: ())
        }

        private let output: Upstream.Output

        override func receive(newValue: Upstream.Output) -> Reduction {
            if newValue == output {
                result = true
                return .finished
            }
            return .continue
        }

        override var description: String { "Contains" }
    }
}

extension Publishers.ContainsWhere {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Bool, Upstream.Failure, (Upstream.Output) -> Bool
    > where Downstream.Input == Bool, Downstream.Failure == Upstream.Failure {
        fileprivate init(downstream: Downstream, predicate: @escaping (Upstream.Output) -> Bool) {
            super.init(downstream: downstream, initial: false, reduce: predicate)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            if reduce(newValue) {
                result = true
                return .finished
            }
            return .continue
        }

        override var description: String { "ContainsWhere" }
    }
}

extension Publishers.TryContainsWhere {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Bool, Upstream.Failure,
        (Upstream.Output) throws -> Bool
    > where Downstream.Input == Bool, Downstream.Failure == Error {
        fileprivate init(
            downstream: Downstream,
            predicate: @escaping (Upstream.Output) throws -> Bool
        ) {
            super.init(downstream: downstream, initial: false, reduce: predicate)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            do {
                if try reduce(newValue) {
                    result = true
                    return .finished
                }
            } catch {
                return .failure(error)
            }
            return .continue
        }

        override var description: String { "TryContainsWhere" }
    }
}
