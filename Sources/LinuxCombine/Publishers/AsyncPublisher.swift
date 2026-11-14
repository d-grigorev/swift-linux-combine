// Async interop: the official `values` properties and the
// `AsyncPublisher` / `AsyncThrowingPublisher` AsyncSequence wrappers.
//
// Official API verified against Apple DocC (12 member pages: struct
// inits, `Element`, `Iterator`, `makeAsyncIterator()`, `next()`, plus
// the two `Publisher.values` properties).
//
// Semantics (verified by the parity suite): each `next()` pump requests
// `.max(1)` from the upstream; `next()` calls made before the
// subscription arrives accumulate as pending demand and are replayed
// on subscription; completion/cancellation resume all pending
// continuations with `nil` (the throwing variant delivers the failure
// to the first pending continuation instead); task cancellation wraps
// `next()` with a handler that cancels the upstream subscription.

extension Publisher where Failure == Never {

    /// The elements produced by the publisher, as an asynchronous
    /// sequence.
    @available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
    public var values: AsyncPublisher<Self> {
        .init(self)
    }
}

extension Publisher {

    /// The elements produced by the publisher, as a throwing
    /// asynchronous sequence.
    @available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
    public var values: AsyncThrowingPublisher<Self> {
        .init(self)
    }
}

/// A publisher that exposes its elements as an asynchronous sequence.
@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
public struct AsyncPublisher<Upstream: Publisher>: AsyncSequence
where Upstream.Failure == Never {
    public typealias Element = Upstream.Output

    /// The iterator that produces elements of the asynchronous
    /// publisher sequence.
    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = Upstream.Output

        fileprivate let inner: Inner

        /// Produces the next element, or `nil` if the publisher
        /// finishes normally.
        public mutating func next() async -> Element? {
            await withTaskCancellationHandler(
                operation: { [inner] in await inner.next() },
                onCancel: { [inner] in inner.cancel() }
            )
        }
    }

    /// The type of asynchronous iterator that produces elements of
    /// this asynchronous sequence.
    public typealias AsyncIterator = Iterator

    private let publisher: Upstream

    /// Creates a publisher that exposes elements received from an
    /// upstream publisher as an asynchronous sequence.
    public init(_ publisher: Upstream) {
        self.publisher = publisher
    }

    /// Creates the asynchronous iterator that produces elements of
    /// this asynchronous sequence.
    public func makeAsyncIterator() -> Iterator {
        let inner = Iterator.Inner()
        publisher.subscribe(inner)
        return Iterator(inner: inner)
    }
}

@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension AsyncPublisher.Iterator {

    fileprivate final class Inner: Subscriber, Cancellable {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private enum State {
            case awaitingSubscription
            case subscribed(any Subscription)
            case terminal
        }

        private let lock = Lock()
        private var pending: [UnsafeContinuation<Input?, Never>] = []
        private var state = State.awaitingSubscription
        private var pendingDemand = Subscribers.Demand.none

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            let pendingDemand = self.pendingDemand
            self.pendingDemand = .none
            lock.unlock()
            if pendingDemand != .none {
                subscription.request(pendingDemand)
            }
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = state else {
                let pending = self.pending
                self.pending = []
                lock.unlock()
                pending.resumeAllWithNil()
                return .none
            }
            precondition(!pending.isEmpty,
                         "Received an output without requesting demand")
            let continuation = pending.removeFirst()
            lock.unlock()
            continuation.resume(returning: input)
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            state = .terminal
            let pending = self.pending
            self.pending = []
            lock.unlock()
            pending.resumeAllWithNil()
        }

        func cancel() {
            lock.lock()
            let pending = self.pending
            self.pending = []
            guard case let .subscribed(subscription) = state else {
                state = .terminal
                lock.unlock()
                pending.resumeAllWithNil()
                return
            }
            state = .terminal
            lock.unlock()
            subscription.cancel()
            pending.resumeAllWithNil()
        }

        fileprivate func next() async -> Input? {
            await withUnsafeContinuation { continuation in
                lock.lock()
                switch state {
                case .awaitingSubscription:
                    pending.append(continuation)
                    pendingDemand += 1
                    lock.unlock()
                case .subscribed(let subscription):
                    pending.append(continuation)
                    lock.unlock()
                    subscription.request(.max(1))
                case .terminal:
                    lock.unlock()
                    continuation.resume(returning: nil)
                }
            }
        }
    }
}

/// A publisher that exposes its elements as a throwing asynchronous
/// sequence.
@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
public struct AsyncThrowingPublisher<Upstream: Publisher>: AsyncSequence {
    public typealias Element = Upstream.Output

    /// The iterator that produces elements of the asynchronous
    /// publisher sequence.
    public struct Iterator: AsyncIteratorProtocol {
        public typealias Element = Upstream.Output

        fileprivate let inner: Inner

        /// Produces the next element, `nil` if the publisher finishes
        /// normally, or throws the publisher's failure.
        public mutating func next() async throws -> Element? {
            try await withTaskCancellationHandler(
                operation: { [inner] in try await inner.next() },
                onCancel: { [inner] in inner.cancel() }
            )
        }
    }

    /// The type of asynchronous iterator that produces elements of
    /// this asynchronous sequence.
    public typealias AsyncIterator = Iterator

    private let publisher: Upstream

    /// Creates a publisher that exposes elements received from an
    /// upstream publisher as an asynchronous sequence.
    public init(_ publisher: Upstream) {
        self.publisher = publisher
    }

    /// Creates the asynchronous iterator that produces elements of
    /// this asynchronous sequence.
    public func makeAsyncIterator() -> Iterator {
        let inner = Iterator.Inner()
        publisher.subscribe(inner)
        return Iterator(inner: inner)
    }
}

@available(macOS 10.15, iOS 13.0, watchOS 6.0, tvOS 13.0, *)
extension AsyncThrowingPublisher.Iterator {

    fileprivate final class Inner: Subscriber, Cancellable {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private enum State {
            case awaitingSubscription
            case subscribed(any Subscription)
            case terminal(Error?)
        }

        private let lock = Lock()
        private var pending: [UnsafeContinuation<Input?, Error>] = []
        private var state = State.awaitingSubscription
        private var pendingDemand = Subscribers.Demand.none

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            let pendingDemand = self.pendingDemand
            self.pendingDemand = .none
            lock.unlock()
            if pendingDemand != .none {
                subscription.request(pendingDemand)
            }
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = state else {
                let pending = self.pending
                self.pending = []
                lock.unlock()
                pending.resumeAllWithNil()
                return .none
            }
            precondition(!pending.isEmpty,
                         "Received an output without requesting demand")
            let continuation = pending.removeFirst()
            lock.unlock()
            continuation.resume(returning: input)
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            switch state {
            case .awaitingSubscription, .subscribed:
                if let continuation = pending.first {
                    state = .terminal(nil)
                    let remaining = Array(pending.dropFirst())
                    pending = []
                    lock.unlock()
                    switch completion {
                    case .finished:
                        continuation.resume(returning: nil)
                    case .failure(let error):
                        continuation.resume(throwing: error)
                    }
                    remaining.resumeAllWithNil()
                } else {
                    if case .failure(let error) = completion {
                        state = .terminal(error)
                    } else {
                        state = .terminal(nil)
                    }
                    lock.unlock()
                }
            case .terminal:
                let pending = self.pending
                self.pending = []
                lock.unlock()
                pending.resumeAllWithNil()
            }
        }

        func cancel() {
            lock.lock()
            let pending = self.pending
            self.pending = []
            guard case let .subscribed(subscription) = state else {
                state = .terminal(nil)
                lock.unlock()
                pending.resumeAllWithNil()
                return
            }
            state = .terminal(nil)
            lock.unlock()
            subscription.cancel()
            pending.resumeAllWithNil()
        }

        fileprivate func next() async throws -> Input? {
            try await withUnsafeThrowingContinuation { continuation in
                lock.lock()
                switch state {
                case .awaitingSubscription:
                    pending.append(continuation)
                    pendingDemand += 1
                    lock.unlock()
                case .subscribed(let subscription):
                    pending.append(continuation)
                    lock.unlock()
                    subscription.request(.max(1))
                case .terminal(nil):
                    lock.unlock()
                    continuation.resume(returning: nil)
                case .terminal(let error?):
                    state = .terminal(nil)
                    lock.unlock()
                    continuation.resume(throwing: error)
                }
            }
        }
    }
}

private extension Sequence {
    /// Resumes every pending continuation with `nil`.
    func resumeAllWithNil<Output, Failure: Error>()
    where Element == UnsafeContinuation<Output?, Failure> {
        for continuation in self {
            continuation.resume(returning: nil)
        }
    }
}
