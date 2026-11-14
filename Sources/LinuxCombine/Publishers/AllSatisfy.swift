extension Publishers {

    /// A publisher that publishes a single Boolean value that indicates
    /// whether all received elements pass a given predicate.
    ///
    /// Official API: `struct AllSatisfy<Upstream>` with
    /// `init(upstream:predicate:)`.
    ///
    /// Semantics (verified by the parity suite): a failing predicate
    /// cancels the upstream and emits `false`; a clean finish emits
    /// `true`; the first downstream request is gating — the result is
    /// held until it arrives (the upstream is pulled `.unlimited`).
    public struct AllSatisfy<Upstream: Publisher>: Publisher {
        public typealias Output = Bool
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// A closure that evaluates each received element; return `false`
        /// to stop early.
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

    /// A publisher that publishes a single Boolean value that indicates
    /// whether all received elements pass a given error-throwing
    /// predicate.
    ///
    /// Official API: `struct TryAllSatisfy<Upstream>` with
    /// `init(upstream:predicate:)`; `Failure == Error`.
    public struct TryAllSatisfy<Upstream: Publisher>: Publisher {
        public typealias Output = Bool
        public typealias Failure = Error

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// A closure that evaluates each received element; may throw to
        /// fail the stream.
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

extension Publisher {
    /// Publishes a single Boolean value that indicates whether all
    /// received elements pass a given predicate.
    public func allSatisfy(
        _ predicate: @escaping (Output) -> Bool
    ) -> Publishers.AllSatisfy<Self> {
        .init(upstream: self, predicate: predicate)
    }

    /// Publishes a single Boolean value that indicates whether all
    /// received elements pass a given error-throwing predicate.
    public func tryAllSatisfy(
        _ predicate: @escaping (Output) throws -> Bool
    ) -> Publishers.TryAllSatisfy<Self> {
        .init(upstream: self, predicate: predicate)
    }
}

extension Publishers.AllSatisfy {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Bool, Upstream.Failure, (Upstream.Output) -> Bool
    > where Downstream.Input == Bool, Downstream.Failure == Upstream.Failure {
        fileprivate init(downstream: Downstream, predicate: @escaping (Upstream.Output) -> Bool) {
            super.init(downstream: downstream, initial: true, reduce: predicate)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            if !reduce(newValue) {
                result = false
                return .finished
            }
            return .continue
        }

        override var description: String { "AllSatisfy" }
    }
}

extension Publishers.TryAllSatisfy {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Bool, Upstream.Failure,
        (Upstream.Output) throws -> Bool
    > where Downstream.Input == Bool, Downstream.Failure == Error {
        fileprivate init(
            downstream: Downstream,
            predicate: @escaping (Upstream.Output) throws -> Bool
        ) {
            super.init(downstream: downstream, initial: true, reduce: predicate)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            do {
                if try !reduce(newValue) {
                    result = false
                    return .finished
                }
            } catch {
                return .failure(error)
            }
            return .continue
        }

        override var description: String { "TryAllSatisfy" }
    }
}
