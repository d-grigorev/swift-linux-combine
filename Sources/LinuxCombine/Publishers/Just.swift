/// A publisher that emits a single value and then finishes.
///
/// Official API verified against Apple DocC: `struct Just<Output>`,
/// `init(_:)`, `let output`, `append(_:)`/`prepend(_:)` (returning
/// `Publishers.Sequence`), conditional `Equatable`.
///
/// The single value is delivered on the *first* request of any demand,
/// followed immediately by `.finished`. (The Just operator sugar — map,
/// filter, etc. — is tracked as backlog: it returns `Optional.Publisher` /
/// `Result.Publisher`, which are outside the current core scope.)
public struct Just<Output>: Publisher {
    public typealias Failure = Never

    /// The only element this publisher emits.
    public let output: Output

    public init(_ output: Output) {
        self.output = output
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Never {
        subscriber.receive(subscription: Inner(value: output, downstream: subscriber))
    }

    /// Appends the given elements after this publisher's element.
    public func append(_ elements: Output...) -> Publishers.Sequence<[Output], Never> {
        .init(sequence: [output] + elements)
    }

    /// Appends the elements of the given sequence after this publisher's element.
    public func append<S: Sequence>(_ elements: S) -> Publishers.Sequence<[Output], Never>
    where S.Element == Output {
        .init(sequence: [output] + elements)
    }

    /// Prepends the given elements before this publisher's element.
    public func prepend(_ elements: Output...) -> Publishers.Sequence<[Output], Never> {
        .init(sequence: elements + [output])
    }

    /// Prepends the elements of the given sequence before this publisher's element.
    public func prepend<S: Sequence>(_ elements: S) -> Publishers.Sequence<[Output], Never>
    where S.Element == Output {
        .init(sequence: elements + [output])
    }
}

extension Just: Equatable where Output: Equatable {}

extension Just {

    /// Single-shot subscription: the value goes out on the first request,
    /// regardless of its size, then `.finished`.
    private final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Never {
        // NOTE: audited for thread safety — Combine does not lock here.

        private var downstream: Downstream?
        private let value: Output

        fileprivate init(value: Output, downstream: Downstream) {
            self.value = value
            self.downstream = downstream
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            let downstream = self.downstream
            self.downstream = nil
            guard let downstream else { return }
            _ = downstream.receive(value)
            downstream.receive(completion: .finished)
        }

        func cancel() {
            downstream = nil
        }

        var description: String { "Just" }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            Mirror(self, unlabeledChildren: CollectionOfOne(value))
        }
    }
}

// MARK: - Operator sugar (official 45-member surface; the backend types
//         Optional.Publisher / Result.Publisher are DocC-blind —
//         docs/QUIRKS.md C2 — the borrowed JustTests bodies pin them)

extension Just where Output: Comparable {

    public func min() -> Just<Output> {
        self
    }

    public func max() -> Just<Output> {
        self
    }
}

extension Just where Output: Equatable {

    public func contains(_ output: Output) -> Just<Bool> {
        .init(self.output == output)
    }

    public func removeDuplicates() -> Just<Output> {
        self
    }
}

extension Just {

    public func allSatisfy(_ predicate: (Output) -> Bool) -> Just<Bool> {
        .init(predicate(output))
    }

    public func tryAllSatisfy(
        _ predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(Result { try predicate(output) })
    }

    public func contains(where predicate: (Output) -> Bool) -> Just<Bool> {
        .init(predicate(output))
    }

    public func tryContains(
        where predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(Result { try predicate(output) })
    }

    public func collect() -> Just<[Output]> {
        .init([output])
    }

    public func min(by areInIncreasingOrder: (Output, Output) -> Bool) -> Just<Output> {
        self
    }

    public func max(by areInIncreasingOrder: (Output, Output) -> Bool) -> Just<Output> {
        self
    }

    public func count() -> Just<Int> {
        .init(1)
    }

    public func dropFirst(_ count: Int = 1) -> Optional<Output>.Publisher {
        precondition(count >= 0, "count must not be negative")
        return .init(count > 0 ? nil : output)
    }

    public func drop(while predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(predicate(output) ? nil : output)
    }

    public func first() -> Just<Output> {
        self
    }

    public func first(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(predicate(output) ? output : nil)
    }

    public func last() -> Just<Output> {
        self
    }

    public func last(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(predicate(output) ? output : nil)
    }

    public func ignoreOutput() -> Empty<Output, Never> {
        .init()
    }

    public func map<ElementOfResult>(_ transform: (Output) -> ElementOfResult)
    -> Just<ElementOfResult> {
        .init(transform(output))
    }

    public func tryMap<ElementOfResult>(
        _ transform: (Output) throws -> ElementOfResult
    ) -> Result<ElementOfResult, Error>.Publisher {
        .init(Result { try transform(output) })
    }

    public func compactMap<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult?
    ) -> Optional<ElementOfResult>.Publisher {
        .init(transform(output))
    }

    public func filter(_ isIncluded: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(isIncluded(output) ? output : nil)
    }

    public func output(at index: Int) -> Optional<Output>.Publisher {
        precondition(index >= 0, "index must not be negative")
        return .init(index == 0 ? output : nil)
    }

    public func output<R: Swift.RangeExpression>(
        in range: R
    ) -> Optional<Output>.Publisher where R.Bound == Int {
        let range = range.relative(to: 0 ..< Int.max)
        return .init(range.lowerBound == 0 ? output : nil)
    }

    public func prefix(_ maxLength: Int) -> Optional<Output>.Publisher {
        precondition(maxLength >= 0, "maxLength must not be negative")
        return .init(maxLength > 0 ? output : nil)
    }

    public func prefix(while predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(predicate(output) ? output : nil)
    }

    public func setFailureType<NewFailure: Error>(
        to failureType: NewFailure.Type
    ) -> Result<Output, NewFailure>.Publisher {
        .init(output)
    }

    public func mapError<NewFailure: Error>(
        _ transform: (Never) -> NewFailure
    ) -> Result<Output, NewFailure>.Publisher {
        .init(output)
    }

    public func removeDuplicates(by predicate: (Output, Output) -> Bool) -> Just<Output> {
        self
    }

    public func tryRemoveDuplicates(
        by predicate: (Output, Output) throws -> Bool
    ) -> Result<Output, Error>.Publisher {
        .init(Result { _ = try predicate(output, output) ; return output })
    }

    public func replaceError(with output: Output) -> Just<Output> {
        self
    }

    public func replaceEmpty(with output: Output) -> Just<Output> {
        self
    }

    public func retry(_ times: Int) -> Just<Output> {
        self
    }

    public func reduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: (Accumulator, Output) -> Accumulator
    ) -> Result<Accumulator, Never>.Publisher {
        .init(nextPartialResult(initialResult, output))
    }

    public func tryReduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: (Accumulator, Output) throws -> Accumulator
    ) -> Result<Accumulator, Error>.Publisher {
        .init(Result { try nextPartialResult(initialResult, output) })
    }

    public func scan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: (ElementOfResult, Output) -> ElementOfResult
    ) -> Result<ElementOfResult, Never>.Publisher {
        .init(nextPartialResult(initialResult, output))
    }

    public func tryScan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: (ElementOfResult, Output) throws -> ElementOfResult
    ) -> Result<ElementOfResult, Error>.Publisher {
        .init(Result { try nextPartialResult(initialResult, output) })
    }
}
