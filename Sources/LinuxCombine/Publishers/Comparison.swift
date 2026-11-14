extension Publisher where Output: Comparable {

    /// Publishes the minimum value received from the upstream publisher,
    /// after it finishes.
    ///
    /// Official API (verified against Apple DocC): `min()` and `max()`
    /// return `Publishers.Comparison<Self>`; the first downstream request
    /// is gating (the upstream is pulled `.unlimited`).
    public func min() -> Publishers.Comparison<Self> {
        max(by: >)
    }

    /// Publishes the maximum value received from the upstream publisher,
    /// after it finishes.
    public func max() -> Publishers.Comparison<Self> {
        max(by: <)
    }
}

extension Publisher {

    /// Publishes the minimum value using the provided ordering closure.
    public func min(
        by areInIncreasingOrder: @escaping (Output, Output) -> Bool
    ) -> Publishers.Comparison<Self> {
        max(by: { areInIncreasingOrder($1, $0) })
    }

    /// Publishes the maximum value using the provided ordering closure.
    public func max(
        by areInIncreasingOrder: @escaping (Output, Output) -> Bool
    ) -> Publishers.Comparison<Self> {
        .init(upstream: self, areInIncreasingOrder: areInIncreasingOrder)
    }

    /// Publishes the minimum value using the provided error-throwing
    /// ordering closure.
    public func tryMin(
        by areInIncreasingOrder: @escaping (Output, Output) throws -> Bool
    ) -> Publishers.TryComparison<Self> {
        tryMax(by: { try areInIncreasingOrder($1, $0) })
    }

    /// Publishes the maximum value using the provided error-throwing
    /// ordering closure.
    public func tryMax(
        by areInIncreasingOrder: @escaping (Output, Output) throws -> Bool
    ) -> Publishers.TryComparison<Self> {
        .init(upstream: self, areInIncreasingOrder: areInIncreasingOrder)
    }

    /// Republishes the maximum element under the given ordering closure.
    ///
    /// Note: this official method has no DocC page in the current
    /// documentation JSON (404 — docs/QUIRKS.md §C C8); the signature is
    /// pinned by the official `init(upstream:areInIncreasingOrder:)` and
    /// own tests (`CombineTests/ComparisonSugarTests`).
    public func comparison(
        areInIncreasingOrder: @escaping (Output, Output) -> Bool
    ) -> Publishers.Comparison<Self> {
        .init(upstream: self, areInIncreasingOrder: areInIncreasingOrder)
    }

    /// Republishes the maximum element under the given error-throwing
    /// ordering closure.
    ///
    /// Note: no DocC page in the current documentation JSON either
    /// (docs/QUIRKS.md §C C8); own tests pin the behavior.
    public func tryComparison(
        areInIncreasingOrder: @escaping (Output, Output) throws -> Bool
    ) -> Publishers.TryComparison<Self> {
        .init(upstream: self, areInIncreasingOrder: areInIncreasingOrder)
    }
}

extension Publishers {

    /// A publisher that republishes items from another publisher only if
    /// each new item is in increasing order from the previously-published
    /// item.
    ///
    /// Official API: `struct Comparison<Upstream>` with
    /// `init(upstream:areInIncreasingOrder:)`.
    ///
    /// Semantics (verified by the parity suite): folds the running
    /// maximum under the closure; the empty stream emits only
    /// `.finished`; the result is held until the first downstream
    /// request.
    public struct Comparison<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher that this publisher receives elements from.
        public let upstream: Upstream

        /// A closure that receives two elements and returns `true` if
        /// they are in increasing order.
        public let areInIncreasingOrder: (Upstream.Output, Upstream.Output) -> Bool

        public init(
            upstream: Upstream,
            areInIncreasingOrder: @escaping (Upstream.Output, Upstream.Output) -> Bool
        ) {
            self.upstream = upstream
            self.areInIncreasingOrder = areInIncreasingOrder
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                areInIncreasingOrder: areInIncreasingOrder
            ))
        }
    }

    /// A publisher that republishes items from another publisher only if
    /// each new item is in increasing order from the previously-published
    /// item, and fails if the ordering logic throws an error.
    ///
    /// Official API: `struct TryComparison<Upstream>` with
    /// `init(upstream:areInIncreasingOrder:)`; `Failure == Error`.
    public struct TryComparison<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Error

        /// The publisher that this publisher receives elements from.
        public let upstream: Upstream

        /// A closure that receives two elements and returns `true` if
        /// they are in increasing order.
        public let areInIncreasingOrder: (Upstream.Output, Upstream.Output) throws -> Bool

        public init(
            upstream: Upstream,
            areInIncreasingOrder: @escaping (Upstream.Output, Upstream.Output) throws -> Bool
        ) {
            self.upstream = upstream
            self.areInIncreasingOrder = areInIncreasingOrder
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                areInIncreasingOrder: areInIncreasingOrder
            ))
        }
    }
}

extension Publishers.Comparison {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Upstream.Output, Upstream.Failure,
        (Upstream.Output, Upstream.Output) -> Bool
    > where Downstream.Input == Upstream.Output,
            Downstream.Failure == Upstream.Failure {
        fileprivate init(
            downstream: Downstream,
            areInIncreasingOrder: @escaping (Upstream.Output, Upstream.Output) -> Bool
        ) {
            super.init(downstream: downstream, initial: nil, reduce: areInIncreasingOrder)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            if let result {
                if reduce(result, newValue) {
                    self.result = newValue
                }
            } else {
                result = newValue
            }
            return .continue
        }

        override var description: String { "Comparison" }
    }
}

extension Publishers.TryComparison {

    private final class Inner<Downstream: Subscriber>: ReduceProducer<
        Downstream, Upstream.Output, Upstream.Output, Upstream.Failure,
        (Upstream.Output, Upstream.Output) throws -> Bool
    > where Downstream.Input == Upstream.Output, Downstream.Failure == Error {
        fileprivate init(
            downstream: Downstream,
            areInIncreasingOrder: @escaping (Upstream.Output, Upstream.Output) throws -> Bool
        ) {
            super.init(downstream: downstream, initial: nil, reduce: areInIncreasingOrder)
        }

        override func receive(newValue: Upstream.Output) -> Reduction {
            do {
                if let result {
                    if try reduce(result, newValue) {
                        self.result = newValue
                    }
                } else {
                    result = newValue
                }
                return .continue
            } catch {
                return .failure(error)
            }
        }

        override var description: String { "TryComparison" }
    }
}
