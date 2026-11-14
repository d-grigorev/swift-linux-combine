extension Optional {

    /// The type of a Combine publisher that publishes the value of a
    /// Swift optional instance to each subscriber exactly once, if the
    /// instance has any value at all.
    ///
    /// Official API (DocC has no page for it — docs/QUIRKS.md C2 — the
    /// shape is pinned by the 41 official Just/Sequence sugar member
    /// declarations that return it, by the compatibility branch of the
    /// borrowed tests, and by `OptionalPublisherTests`):
    /// `Optional.Publisher` with `output`, `init(_:)`, plus the
    /// `Optional.publisher` property and conditional `Equatable`.
    ///
    /// Semantics (verified by the parity suite): a non-nil value is
    /// delivered on the first non-zero request, then `.finished`; a nil
    /// optional grants `Subscriptions.empty` and finishes immediately;
    /// description "Optional"; the single-shot inner is lock-free (as
    /// upstream).
    public struct Publisher: LinuxCombine.Publisher {
        /// The kind of value published by this publisher — the type
        /// wrapped by the optional.
        public typealias Output = Wrapped

        /// The kind of error this publisher might publish — the optional
        /// publisher never produces errors.
        public typealias Failure = Never

        /// The output to deliver to each subscriber.
        public let output: Wrapped?

        /// Creates a publisher to emit the value of the optional, or to
        /// finish immediately if the optional doesn't have a value.
        public init(_ output: Output?) {
            self.output = output
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            if let output {
                subscriber.receive(subscription: Inner(value: output,
                                                        downstream: subscriber))
            } else {
                subscriber.receive(subscription: Subscriptions.empty)
                subscriber.receive(completion: .finished)
            }
        }
    }

    /// A publisher that publishes this optional's value, or finishes
    /// immediately when it is `nil`.
    public var publisher: Publisher {
        Publisher(self)
    }
}

extension Optional.Publisher {

    /// Single-shot subscription: the value goes out on the first
    /// non-zero request, then `.finished`.
    private final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Wrapped {
        // NOTE: audited for thread safety — Combine does not lock here.

        private var downstream: Downstream?
        private let output: Wrapped

        init(value: Wrapped, downstream: Downstream) {
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

        var description: String { "Optional" }

        var customMirror: Mirror {
            Mirror(self, unlabeledChildren: CollectionOfOne(output))
        }

        var playgroundDescription: Any { description }
    }
}

extension Optional.Publisher: Equatable where Wrapped: Equatable {}

// MARK: - Operator sugar (official surface pinned by
//         `OptionalPublisherTests`)

extension Optional.Publisher where Wrapped: Equatable {

    public func contains(_ output: Output) -> Optional<Bool>.Publisher {
        .init(self.output.map { $0 == output })
    }

    public func removeDuplicates() -> Optional<Wrapped>.Publisher {
        self
    }
}

extension Optional.Publisher where Wrapped: Comparable {

    public func min() -> Optional<Wrapped>.Publisher {
        self
    }

    public func max() -> Optional<Wrapped>.Publisher {
        self
    }
}

extension Optional.Publisher {

    public func allSatisfy(_ predicate: (Output) -> Bool) -> Optional<Bool>.Publisher {
        .init(output.map(predicate))
    }

    public func collect() -> Optional<[Output]>.Publisher {
        .init(output.map { [$0] } ?? [])
    }

    public func compactMap<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult?
    ) -> Optional<ElementOfResult>.Publisher {
        .init(output.flatMap(transform))
    }

    public func min(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Optional<Output>.Publisher {
        self
    }

    public func max(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Optional<Output>.Publisher {
        self
    }

    public func contains(where predicate: (Output) -> Bool) -> Optional<Bool>.Publisher {
        .init(output.map(predicate))
    }

    public func count() -> Optional<Int>.Publisher {
        .init(output.map { _ in 1 })
    }

    public func dropFirst(_ count: Int = 1) -> Optional<Output>.Publisher {
        precondition(count >= 0, "count must not be negative")
        return .init(output.flatMap { count == 0 ? $0 : nil })
    }

    public func drop(while predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(output.flatMap { predicate($0) ? nil : $0 })
    }

    public func first() -> Optional<Output>.Publisher {
        self
    }

    public func first(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(output.flatMap { predicate($0) ? $0 : nil })
    }

    public func last() -> Optional<Output>.Publisher {
        self
    }

    public func last(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(output.flatMap { predicate($0) ? $0 : nil })
    }

    public func filter(_ isIncluded: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(output.flatMap { isIncluded($0) ? $0 : nil })
    }

    public func ignoreOutput() -> Empty<Output, Failure> {
        .init()
    }

    public func map<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult
    ) -> Optional<ElementOfResult>.Publisher {
        .init(output.map(transform))
    }

    public func output(at index: Int) -> Optional<Output>.Publisher {
        precondition(index >= 0, "index must not be negative")
        return .init(output.flatMap { index == 0 ? $0 : nil })
    }

    public func output<R: Swift.RangeExpression>(
        in range: R
    ) -> Optional<Output>.Publisher where R.Bound == Int {
        let range = range.relative(to: 0 ..< Int.max)
        precondition(range.lowerBound >= 0, "lowerBound must not be negative")
        // Apple's implementation carries this precondition as well.
        precondition(range.upperBound < .max - 1)
        return .init(
            output.flatMap { (range.lowerBound == 0 && range.upperBound != 0) ? $0 : nil }
        )
    }

    public func prefix(_ maxLength: Int) -> Optional<Output>.Publisher {
        precondition(maxLength >= 0, "maxLength must not be negative")
        return .init(output.flatMap { maxLength > 0 ? $0 : nil })
    }

    public func prefix(while predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(output.flatMap { predicate($0) ? $0 : nil })
    }

    public func reduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: (Accumulator, Output) -> Accumulator
    ) -> Optional<Accumulator>.Publisher {
        .init(output.map { nextPartialResult(initialResult, $0) })
    }

    public func scan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: (ElementOfResult, Output) -> ElementOfResult
    ) -> Optional<ElementOfResult>.Publisher {
        .init(output.map { nextPartialResult(initialResult, $0) })
    }

    public func removeDuplicates(
        by predicate: (Output, Output) -> Bool
    ) -> Optional<Output>.Publisher {
        self
    }

    public func replaceError(with output: Output) -> Optional<Output>.Publisher {
        self
    }

    public func replaceEmpty(with output: Output) -> Just<Output> {
        .init(self.output ?? output)
    }

    public func retry(_ times: Int) -> Optional<Output>.Publisher {
        self
    }
}
