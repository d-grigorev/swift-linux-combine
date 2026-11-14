extension Publishers {

    /// A publisher that publishes elements from a sequence.
    ///
    /// Official API verified against Apple DocC: `struct Sequence<Elements,
    /// Failure>`, `let sequence`, `init(sequence:)`, conditional `Equatable`
    /// (where `Elements: Equatable`), plus the `append`/`prepend` family.
    /// The `Sequence.publisher` property is official Combine API as well
    /// (not present in the Combine DocC tree; verified via OpenCombine
    /// parity).
    public struct Sequence<Elements: Swift.Sequence, Failure: Error>: Publisher {

        public typealias Output = Elements.Element

        /// The sequence of elements to publish.
        public let sequence: Elements

        public init(sequence: Elements) {
            self.sequence = sequence
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber, sequence: sequence)
            if inner.isExhausted {
                // Empty sequence: complete immediately, not demand-gated.
                subscriber.receive(subscription: Subscriptions.empty)
                subscriber.receive(completion: .finished)
                inner.cancel()
            } else {
                subscriber.receive(subscription: inner)
            }
        }
    }

    // The append/prepend family requires a concatenatable collection so the
    // result stays `Elements` (official constraint lives on the extension,
    // which DocC does not render in the member declarations).
}

extension Publishers.Sequence where Elements: RangeReplaceableCollection {

    /// Appends the given elements after this sequence's elements.
    public func append(_ elements: Output...) -> Publishers.Sequence<Elements, Failure> {
        .init(sequence: sequence + elements)
    }

    /// Appends the elements of the given sequence after this sequence's elements.
    public func append<S: Swift.Sequence>(
        _ elements: S
    ) -> Publishers.Sequence<Elements, Failure>
    where S.Element == Elements.Element {
        .init(sequence: sequence + elements)
    }

    /// Appends another sequence publisher's elements after this one's.
    public func append(
        _ publisher: Publishers.Sequence<Elements, Failure>
    ) -> Publishers.Sequence<Elements, Failure> {
        .init(sequence: sequence + publisher.sequence)
    }

    /// Prepends the given elements before this sequence's elements.
    public func prepend(_ elements: Output...) -> Publishers.Sequence<Elements, Failure> {
        .init(sequence: elements + sequence)
    }

    /// Prepends the elements of the given sequence before this sequence's elements.
    public func prepend<S: Swift.Sequence>(
        _ elements: S
    ) -> Publishers.Sequence<Elements, Failure>
    where S.Element == Elements.Element {
        .init(sequence: elements + sequence)
    }

    /// Prepends another sequence publisher's elements before this one's.
    public func prepend(
        _ publisher: Publishers.Sequence<Elements, Failure>
    ) -> Publishers.Sequence<Elements, Failure> {
        .init(sequence: publisher.sequence + sequence)
    }
}

extension Publishers.Sequence: Equatable where Elements: Equatable {}

extension Publishers.Sequence {

    /// Demand-driven iteration over the elements; `.finished` follows the
    /// last element as soon as it is delivered.
    private final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Elements.Element, Downstream.Failure == Failure {

        private var sequence: Elements?
        private var downstream: Downstream?
        private var iterator: Elements.Iterator
        private var next: Elements.Element?
        private var pendingDemand = Subscribers.Demand.none
        private var recursion = false
        private let lock = Lock()

        fileprivate init(downstream: Downstream, sequence: Elements) {
            self.sequence = sequence
            self.downstream = downstream
            iterator = sequence.makeIterator()
            next = iterator.next()
        }

        fileprivate var isExhausted: Bool {
            next == nil
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard downstream != nil else {
                lock.unlock()
                return
            }
            pendingDemand += demand
            if recursion {
                lock.unlock()
                return
            }

            while let downstream, pendingDemand > 0 {
                if let current = next {
                    pendingDemand -= 1
                    // Stepping the iterator under the lock mirrors Combine;
                    // pathological single-pass Sequences could deadlock here.
                    let next = iterator.next()
                    recursion = true
                    lock.unlock()
                    let additionalDemand = downstream.receive(current)
                    lock.lock()
                    recursion = false
                    pendingDemand += additionalDemand
                    self.next = next
                }

                if next == nil {
                    self.downstream = nil
                    sequence = nil
                    lock.unlock()
                    downstream.receive(completion: .finished)
                    return
                }
            }

            lock.unlock()
        }

        func cancel() {
            lock.lock()
            downstream = nil
            sequence = nil
            lock.unlock()
        }

        var description: String {
            lock.lock()
            defer { lock.unlock() }
            return sequence.map { String(describing: $0) } ?? "Sequence"
        }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("sequence", sequence ?? [Elements.Element]()),
            ]
            return Mirror(self, children: children)
        }
    }
}

extension Swift.Sequence {
    /// A publisher that publishes elements from this sequence, never failing.
    public var publisher: Publishers.Sequence<Self, Never> {
        .init(sequence: self)
    }
}

// MARK: - Operator sugar (official 44-member surface; extension-level
//         constraints are hidden in DocC (C3) — mirrored from the
//         sanctioned consult and pinned by the restored JustTests /
//         SequenceTests bodies)

extension Publishers.Sequence where Failure == Never {

    public func min(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Optional<Output>.Publisher {
        .init(sequence.min(by: areInIncreasingOrder))
    }

    public func max(
        by areInIncreasingOrder: (Output, Output) -> Bool
    ) -> Optional<Output>.Publisher {
        .init(sequence.max(by: areInIncreasingOrder))
    }

    public func first(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(sequence.first(where: predicate))
    }
}

extension Publishers.Sequence {

    public func allSatisfy(_ predicate: (Output) -> Bool) -> Result<Bool, Failure>.Publisher {
        .init(sequence.allSatisfy(predicate))
    }

    public func tryAllSatisfy(
        _ predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(Result { try sequence.allSatisfy(predicate) })
    }

    public func collect() -> Result<[Output], Failure>.Publisher {
        .init(Array(sequence))
    }

    public func compactMap<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult?
    ) -> Publishers.Sequence<[ElementOfResult], Failure> {
        .init(sequence: sequence.compactMap(transform))
    }

    public func contains(where predicate: (Output) -> Bool) -> Result<Bool, Failure>.Publisher {
        .init(sequence.contains(where: predicate))
    }

    public func tryContains(
        where predicate: (Output) throws -> Bool
    ) -> Result<Bool, Error>.Publisher {
        .init(Result { try sequence.contains(where: predicate) })
    }

    public func drop(
        while predicate: (Output) -> Bool
    ) -> Publishers.Sequence<DropWhileSequence<Elements>, Failure> {
        .init(sequence: sequence.drop(while: predicate))
    }

    public func dropFirst(
        _ count: Int = 1
    ) -> Publishers.Sequence<DropFirstSequence<Elements>, Failure> {
        .init(sequence: sequence.dropFirst(count))
    }

    public func filter(
        _ isIncluded: (Output) -> Bool
    ) -> Publishers.Sequence<[Output], Failure> {
        .init(sequence: sequence.filter(isIncluded))
    }

    public func ignoreOutput() -> Empty<Output, Failure> {
        .init()
    }

    public func map<ElementOfResult>(
        _ transform: (Output) -> ElementOfResult
    ) -> Publishers.Sequence<[ElementOfResult], Failure> {
        .init(sequence: sequence.map(transform))
    }

    public func prefix(
        _ maxLength: Int
    ) -> Publishers.Sequence<PrefixSequence<Elements>, Failure> {
        .init(sequence: sequence.prefix(maxLength))
    }

    public func prefix(
        while predicate: (Output) -> Bool
    ) -> Publishers.Sequence<[Output], Failure> {
        .init(sequence: sequence.prefix(while: predicate))
    }

    public func reduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: @escaping (Accumulator, Output) -> Accumulator
    ) -> Result<Accumulator, Failure>.Publisher {
        .init(sequence.reduce(initialResult, nextPartialResult))
    }

    public func tryReduce<Accumulator>(
        _ initialResult: Accumulator,
        _ nextPartialResult: @escaping (Accumulator, Output) throws -> Accumulator
    ) -> Result<Accumulator, Error>.Publisher {
        .init(Result { try sequence.reduce(initialResult, nextPartialResult) })
    }

    public func replaceNil<ElementOfResult>(
        with output: ElementOfResult
    ) -> Publishers.Sequence<[Output], Failure> where Output == ElementOfResult? {
        .init(sequence: sequence.map { $0 ?? output })
    }

    public func scan<ElementOfResult>(
        _ initialResult: ElementOfResult,
        _ nextPartialResult: @escaping (ElementOfResult, Output) -> ElementOfResult
    ) -> Publishers.Sequence<[ElementOfResult], Failure> {
        var accumulator = initialResult
        return .init(sequence: sequence.map {
            accumulator = nextPartialResult(accumulator, $0)
            return accumulator
        })
    }

    public func setFailureType<NewFailure: Error>(
        to error: NewFailure.Type
    ) -> Publishers.Sequence<Elements, NewFailure> {
        .init(sequence: sequence)
    }
}

extension Publishers.Sequence where Output: Equatable {

    public func removeDuplicates() -> Publishers.Sequence<[Output], Failure> {
        var previous: Output?
        var result = [Output]()
        for element in sequence where element != previous {
            result.append(element)
            previous = element
        }
        return .init(sequence: result)
    }

    public func contains(_ output: Output) -> Result<Bool, Failure>.Publisher {
        .init(sequence.contains(output))
    }
}

extension Publishers.Sequence where Failure == Never, Output: Comparable {

    public func min() -> Optional<Output>.Publisher {
        .init(sequence.min())
    }

    public func max() -> Optional<Output>.Publisher {
        .init(sequence.max())
    }
}

extension Publishers.Sequence where Elements: Collection, Failure == Never {

    public func first() -> Optional<Output>.Publisher {
        .init(sequence.first)
    }

    public func output(at index: Elements.Index) -> Optional<Output>.Publisher {
        .init(sequence.indices.contains(index) ? sequence[index] : nil)
    }
}

extension Publishers.Sequence where Elements: Collection {

    public func count() -> Result<Int, Failure>.Publisher {
        .init(sequence.count)
    }

    public func output(
        in range: Range<Elements.Index>
    ) -> Publishers.Sequence<[Output], Failure> {
        .init(sequence: Array(sequence[range]))
    }
}

extension Publishers.Sequence where Elements: BidirectionalCollection, Failure == Never {

    public func last() -> Optional<Output>.Publisher {
        .init(sequence.last)
    }

    public func last(where predicate: (Output) -> Bool) -> Optional<Output>.Publisher {
        .init(sequence.last(where: predicate))
    }
}

extension Publishers.Sequence where Elements: RandomAccessCollection, Failure == Never {

    public func output(at index: Elements.Index) -> Optional<Output>.Publisher {
        .init(sequence.indices.contains(index) ? sequence[index] : nil)
    }

    public func count() -> Just<Int> {
        .init(sequence.count)
    }
}
