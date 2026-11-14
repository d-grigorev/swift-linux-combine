extension Result {

    /// A publisher that publishes an output to each subscriber exactly
    /// once then finishes, or fails immediately without producing any
    /// elements.
    ///
    /// Official API (DocC has no page — docs/QUIRKS.md C2 — shape pinned
    /// by the official Just/Sequence sugar member declarations returning
    /// it, the compatibility branch of the borrowed tests, and
    /// `ResultPublisherTests`): `Result.Publisher` with `result`,
    /// `init(_ result:)` / `init(_ output:)` / `init(_ failure:)`, plus
    /// the `Result.publisher` property and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): a success value is
    /// delivered on the first non-zero request, then `.finished`; a
    /// failure grants `Subscriptions.empty` and fails immediately;
    /// description "Once"; the single-shot inner is lock-free (as
    /// upstream).
    public struct Publisher: LinuxCombine.Publisher {
        public typealias Output = Success
        // `Failure` resolves to Result's generic parameter — declared
        // implicitly, as in the reference implementation.

        /// The result to deliver to each subscriber.
        public let result: Result

        /// Creates a publisher that delivers the specified result.
        public init(_ result: Result) {
            self.result = result
        }

        /// Creates a publisher that sends the specified output to all
        /// subscribers and finishes normally.
        public init(_ output: Output) {
            self.init(.success(output))
        }

        /// Creates a publisher that immediately terminates upon
        /// subscription with the given failure.
        public init(_ failure: Failure) {
            self.init(.failure(failure))
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Success, S.Failure == Failure {
            switch result {
            case .success(let value):
                subscriber.receive(subscription: Inner(value: value,
                                                       downstream: subscriber))
            case .failure(let failure):
                subscriber.receive(subscription: Subscriptions.empty)
                subscriber.receive(completion: .failure(failure))
            }
        }
    }

    /// A publisher that publishes this result, or fails immediately.
    public var publisher: Publisher {
        Publisher(self)
    }
}

extension Result.Publisher {

    /// Single-shot subscription: the success value goes out on the
    /// first non-zero request, then `.finished`.
    private final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Success, Downstream.Failure == Failure {
        // NOTE: audited for thread safety — Combine does not lock here.

        private var downstream: Downstream?
        private let output: Success

        init(value: Success, downstream: Downstream) {
            self.output = value
            self.downstream = downstream
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            let downstream = self.downstream
            self.downstream = nil
            guard let downstream else { return }
            _ = downstream.receive(output)
            downstream.receive(completion: .finished)
        }

        func cancel() {
            downstream = nil
        }

        var description: String { "Once" }

        var customMirror: Mirror {
            Mirror(self, unlabeledChildren: CollectionOfOne(output))
        }

        var playgroundDescription: Any { description }
    }
}

extension Result.Publisher: Equatable where Output: Equatable, Failure: Equatable {}

/// The sugar's throwing-map with error erasure to `any Error` (spelled
/// out: a bare `Result { ... }` inside this extension would bind to the
/// outer `Result<Success, Failure>`).
private extension Swift.Result {
    func sugarTryMap<NewSuccess>(
        _ transform: (Success) throws -> NewSuccess
    ) -> Swift.Result<NewSuccess, any Error> {
        switch self {
        case .success(let value):
            do {
                return .success(try transform(value))
            } catch {
                return .failure(error)
            }
        case .failure(let error):
            return .failure(error)
        }
    }
}

// MARK: - Operator sugar (official surface pinned by
//         `ResultPublisherTests`)

extension Result.Publisher where Output: Equatable {

    public func contains(_ output: Output) -> Result<Bool, Failure>.Publisher {
        .init(result.map { $0 == output })
    }

    public func removeDuplicates() -> Result<Output, Failure>.Publisher {
        self
    }
}

extension Result.Publisher where Output: Comparable {

    public func min() -> Result<Output, Failure>.Publisher {
        self
    }

    public func max() -> Result<Output, Failure>.Publisher {
        self
    }
}

extension Result.Publisher {

    public func allSatisfy(
        _ predicate: (Output) -> Bool
    ) -> Result<Bool, Failure>.Publisher {
        .init(result.map(predicate))
    }

    public func tryAllSatisfy(
        _ predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(result.sugarTryMap(predicate))
    }

    public func contains(
        where predicate: (Output) -> Bool
    ) -> Result<Bool, Failure>.Publisher {
        .init(result.map(predicate))
    }

    public func tryContains(
        where predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(result.sugarTryMap(predicate))
    }

    public func collect() -> Result<[Output], Failure>.Publisher {
        .init(result.map { [$0] })
    }

    public func min(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Result<Output, Failure>.Publisher {
        self
    }

    public func tryMin(
        by areInIncreasingOrder: (Output, Output) throws -> Bool
    ) -> Result<Output, Error>.Publisher {
        .init(result.sugarTryMap { _ = try areInIncreasingOrder($0, $0) ; return $0 })
    }

    public func max(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Result<Output, Failure>.Publisher {
        self
    }

    public func tryMax(
        by areInIncreasingOrder: (Output, Output) throws -> Bool
    ) -> Result<Output, Error>.Publisher {
        .init(result.sugarTryMap { _ = try areInIncreasingOrder($0, $0) ; return $0 })
    }

    public func count() -> Result<Int, Failure>.Publisher {
        .init(result.map { _ in 1 })
    }

    public func first() -> Result<Output, Failure>.Publisher {
        self
    }

    public func last() -> Result<Output, Failure>.Publisher {
        self
    }

    public func ignoreOutput() -> Empty<Output, Failure> {
        .init()
    }

    public func map<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult
    ) -> Result<ElementOfResult, Failure>.Publisher {
        .init(result.map(transform))
    }

    public func tryMap<ElementOfResult>(
        _ transform: (Output) throws -> ElementOfResult
    ) -> Result<ElementOfResult, Error>.Publisher {
        .init(result.sugarTryMap(transform))
    }

    public func mapError<TransformedFailure: Error>(
        _ transform: (Failure) -> TransformedFailure
    ) -> Result<Output, TransformedFailure>.Publisher {
        .init(result.mapError(transform))
    }

    public func removeDuplicates(
        by predicate: (Output, Output) -> Bool
    ) -> Result<Output, Failure>.Publisher {
        self
    }

    public func tryRemoveDuplicates(
        by predicate: (Output, Output) throws -> Bool
    ) -> Result<Output, Error>.Publisher {
        .init(result.sugarTryMap { _ = try predicate($0, $0) ; return $0 })
    }

    public func replaceError(with output: Output) -> Result<Output, Never>.Publisher {
        switch result {
        case .success(let value):
            return .init(.success(value))
        case .failure:
            return .init(.success(output))
        }
    }

    public func replaceEmpty(with output: Output) -> Result<Output, Failure>.Publisher {
        self
    }

    public func retry(_ times: Int) -> Result<Output, Failure>.Publisher {
        self
    }

    public func reduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: (Accumulator, Output) -> Accumulator
    ) -> Result<Accumulator, Failure>.Publisher {
        .init(result.map { nextPartialResult(initialResult, $0) })
    }

    public func tryReduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: (Accumulator, Output) throws -> Accumulator
    ) -> Result<Accumulator, Error>.Publisher {
        .init(result.sugarTryMap { try nextPartialResult(initialResult, $0) })
    }

    public func scan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: (ElementOfResult, Output) -> ElementOfResult
    ) -> Result<ElementOfResult, Failure>.Publisher {
        .init(result.map { nextPartialResult(initialResult, $0) })
    }

    public func tryScan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: (ElementOfResult, Output) throws -> ElementOfResult
    ) -> Result<ElementOfResult, Error>.Publisher {
        .init(result.sugarTryMap { try nextPartialResult(initialResult, $0) })
    }
}

extension Result.Publisher where Failure == Never {

    public func setFailureType<NewFailure: Error>(
        to failureType: NewFailure.Type
    ) -> Result<Output, NewFailure>.Publisher {
        if case .success(let value) = result {
            return .init(.success(value))
        }
        // `Failure == Never`: get() cannot throw.
        return .init(.success(result.get()))
    }
}
