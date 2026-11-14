extension Publishers {

    /// A publisher that publishes the number of elements received from
    /// the upstream publisher.
    ///
    /// Official API: `struct Count<Upstream>` with `init(upstream:)` and
    /// conditional `Equatable`; `Output == Int`.
    ///
    /// Semantics (verified by the parity suite): counts every element;
    /// emits the total when the upstream finishes (held until the first
    /// downstream request).
    public struct Count<Upstream: Publisher>: Publisher {
        public typealias Output = Int
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber))
        }
    }
}

extension Publisher {
    /// Publishes the number of elements received from the upstream
    /// publisher.
    public func count() -> Publishers.Count<Self> {
        .init(upstream: self)
    }
}

extension Publishers.Count: Equatable where Upstream: Equatable {}

extension Publishers.Count {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Int, Upstream.Failure, Void
    > where Downstream.Input == Int, Downstream.Failure == Upstream.Failure {
        fileprivate init(downstream: Downstream) {
            super.init(downstream: downstream, initial: 0, reduce: ())
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            result! += 1
            return .continue
        }

        override var description: String { "Count" }
    }
}
