extension Subscribers {
    // (namespace already declared in Core/Namespaces.swift)
}

extension Publishers {

    /// A publisher that handles errors from an upstream publisher by replacing
    /// the failed publisher with another publisher.
    ///
    /// Official API: `struct Catch<Upstream, NewPublisher>` (Failure comes from
    /// the replacement publisher).
    ///
    /// Semantics (verified by the parity suite): values pass through with
    /// demand bookkeeping; on upstream failure the handler publisher is
    /// subscribed and receives the *unfulfilled* demand; a replacement
    /// completion terminates; requests before the upstream subscription
    /// arrive are fatal (official).
    public struct Catch<Upstream: Publisher, NewPublisher: Publisher>: Publisher
    where Upstream.Output == NewPublisher.Output {
        public typealias Output = Upstream.Output
        public typealias Failure = NewPublisher.Failure

        /// The publisher that this publisher receives elements from.
        public let upstream: Upstream

        /// The closure that produces the replacement publisher on failure.
        public let handler: (Upstream.Failure) -> NewPublisher

        public init(upstream: Upstream, handler: @escaping (Upstream.Failure) -> NewPublisher) {
            self.upstream = upstream
            self.handler = handler
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber, handler: handler)
            upstream.subscribe(Uncaught(inner: inner))
        }
    }

    /// Official API: `struct TryCatch<Upstream, NewPublisher>` (`Failure == Error`).
    public struct TryCatch<Upstream: Publisher, NewPublisher: Publisher>: Publisher
    where Upstream.Output == NewPublisher.Output {
        public typealias Output = Upstream.Output
        public typealias Failure = Error

        public let upstream: Upstream
        public let handler: (Upstream.Failure) throws -> NewPublisher

        public init(
            upstream: Upstream,
            handler: @escaping (Upstream.Failure) throws -> NewPublisher
        ) {
            self.upstream = upstream
            self.handler = handler
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(downstream: subscriber, handler: handler)
            upstream.subscribe(Uncaught(inner: inner))
        }
    }
}

/// Shared pre/post state machine for Catch and TryCatch.
///
/// Mirror children (official): `downstream`, `demand`.
private class CatchCore<Upstream: Publisher, Downstream: Subscriber>
    : Subscription,
      CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
where Downstream.Input == Upstream.Output
{
    typealias Input = Upstream.Output
    typealias Failure = Upstream.Failure

    enum State {
        case pendingPre
        case pre(any Subscription)
        case pendingPost
        case post(any Subscription)
        case cancelled
    }

    let lock = Lock()
    var demand = Subscribers.Demand.none
    var state = State.pendingPre
    let downstream: Downstream

    init(downstream: Downstream) {
        self.downstream = downstream
    }

    func replaceErrorAndSubscribe(_: any Subscription) { fatalError("abstract") }

    // MARK: pre (upstream) side

    func receivePre(subscription: any Subscription) {
        lock.lock()
        guard case .pendingPre = state else {
            lock.unlock()
            subscription.cancel()
            return
        }
        state = .pre(subscription)
        lock.unlock()
        downstream.receive(subscription: self)
    }

    func receivePre(_ input: Input) -> Subscribers.Demand {
        lock.lock()
        demand -= 1
        lock.unlock()
        let newDemand = downstream.receive(input)
        lock.lock()
        demand += newDemand
        lock.unlock()
        return newDemand
    }

    /// - Parameter errorTransformer: maps the upstream error into the
    ///   downstream failure used when the handler itself throws (TryCatch).
    func receivePreFinished() {
        lock.lock()
        switch state {
        case .pre:
            state = .cancelled
            lock.unlock()
            downstream.receive(completion: .finished)
        case .pendingPre, .pendingPost, .post, .cancelled:
            lock.unlock()
        }
    }

    func receivePreFailedEnterPendingPost() -> Bool {
        lock.lock()
        switch state {
        case .pre:
            state = .pendingPost
            lock.unlock()
            return true
        case .cancelled:
            lock.unlock()
            return false
        case .pendingPre, .post, .pendingPost:
            lock.unlock()
            fatalError("Unexpected state: received completion but do not have subscription")
        }
    }

    // MARK: post (replacement) side

    func receivePost(subscription: any Subscription) {
        lock.lock()
        guard case .pendingPost = state else {
            lock.unlock()
            subscription.cancel()
            return
        }
        state = .post(subscription)
        let demand = self.demand
        lock.unlock()
        if demand > 0 {
            subscription.request(demand)
        }
    }

    func receivePost(_ input: Input) -> Subscribers.Demand {
        downstream.receive(input)
    }

    func receivePostFinished() {
        lock.lock()
        guard case .post = state else {
            lock.unlock()
            return
        }
        state = .cancelled
        lock.unlock()
        downstream.receive(completion: .finished)
    }

    func receivePostFailed(_ completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        guard case .post = state else {
            lock.unlock()
            return
        }
        state = .cancelled
        lock.unlock()
        downstream.receive(completion: completion)
    }

    // MARK: Subscription

    func request(_ demand: Subscribers.Demand) {
        assert(demand != .none, "request(_:) demand must not be .none")
        lock.lock()
        switch state {
        case .pendingPre:
            lock.unlock()
            fatalError("Unexpected state: request before subscription sent")
        case let .pre(subscription):
            self.demand += demand
            lock.unlock()
            subscription.request(demand)
        case .pendingPost:
            self.demand += demand
            lock.unlock()
        case let .post(subscription):
            lock.unlock()
            subscription.request(demand)
        case .cancelled:
            lock.unlock()
        }
    }

    func cancel() {
        lock.lock()
        switch state {
        case let .pre(subscription), let .post(subscription):
            state = .cancelled
            lock.unlock()
            subscription.cancel()
        case .pendingPre, .pendingPost, .cancelled:
            lock.unlock()
        }
    }

    var playgroundDescription: Any { description }
    var description: String { fatalError("abstract") }
    var customMirror: Mirror {
        lock.lock()
        defer { lock.unlock() }
        let children: [Mirror.Child] = [
            ("downstream", downstream),
            ("demand", demand),
        ]
        return Mirror(self, children: children)
    }
}

extension Publishers.Catch {

    fileprivate final class Inner<Downstream: Subscriber>: CatchCore<Upstream, Downstream>
    where Downstream.Input == Upstream.Output, Downstream.Failure == NewPublisher.Failure {
        private let handler: (Upstream.Failure) -> NewPublisher

        init(
            downstream: Downstream,
            handler: @escaping (Upstream.Failure) -> NewPublisher
        ) {
            self.handler = handler
            super.init(downstream: downstream)
        }

        func handle(_ upstreamFailure: Upstream.Failure) {
            if receivePreFailedEnterPendingPost() {
                handler(upstreamFailure).subscribe(Caught(inner: self))
            }
        }

        override var description: String { "Catch" }
    }

    /// Upstream-facing subscriber (forwards the inner's identity).
    struct Uncaught<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == NewPublisher.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        fileprivate let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) { inner.receivePre(subscription: subscription) }
        func receive(_ input: Input) -> Subscribers.Demand { inner.receivePre(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished: inner.receivePreFinished()
            case .failure(let error): inner.handle(error)
            }
        }

        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }

    /// Replacement-publisher-facing subscriber.
    struct Caught<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == NewPublisher.Failure {
        typealias Input = NewPublisher.Output
        typealias Failure = NewPublisher.Failure

        fileprivate let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) { inner.receivePost(subscription: subscription) }
        func receive(_ input: Input) -> Subscribers.Demand { inner.receivePost(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished: inner.receivePostFinished()
            case .failure: inner.receivePostFailed(completion)
            }
        }

        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }
}

extension Publishers.TryCatch {

    fileprivate final class Inner<Downstream: Subscriber>: CatchCore<Upstream, Downstream>
    where Downstream.Input == Upstream.Output, Downstream.Failure == Error {
        private let handler: (Upstream.Failure) throws -> NewPublisher

        init(
            downstream: Downstream,
            handler: @escaping (Upstream.Failure) throws -> NewPublisher
        ) {
            self.handler = handler
            super.init(downstream: downstream)
        }

        func handle(_ upstreamFailure: Upstream.Failure) {
            guard receivePreFailedEnterPendingPost() else { return }
            do {
                try handler(upstreamFailure).subscribe(Caught(inner: self))
            } catch let anotherError {
                lock.lock()
                state = .cancelled
                lock.unlock()
                downstream.receive(completion: .failure(anotherError))
            }
        }

        override var description: String { "TryCatch" }
    }

    struct Uncaught<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        fileprivate let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) { inner.receivePre(subscription: subscription) }
        func receive(_ input: Input) -> Subscribers.Demand { inner.receivePre(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished: inner.receivePreFinished()
            case .failure(let error): inner.handle(error)
            }
        }

        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }

    struct Caught<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Error {
        typealias Input = NewPublisher.Output
        typealias Failure = NewPublisher.Failure

        fileprivate let inner: Inner<Downstream>
        var combineIdentifier: CombineIdentifier { inner.combineIdentifier }

        func receive(subscription: any Subscription) { inner.receivePost(subscription: subscription) }
        func receive(_ input: Input) -> Subscribers.Demand { inner.receivePost(input) }
        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished: inner.receivePostFinished()
            case .failure(let error): inner.receivePostFailed(.failure(error))
            }
        }

        var description: String { inner.description }
        var customMirror: Mirror { inner.customMirror }
        var playgroundDescription: Any { inner.playgroundDescription }
    }
}

extension Publisher {
    /// Handles errors from an upstream publisher by replacing it with another publisher.
    public func `catch`<P: Publisher>(_ handler: @escaping (Failure) -> P)
    -> Publishers.Catch<Self, P> where P.Output == Output {
        .init(upstream: self, handler: handler)
    }

    /// Throwing variant of `catch(_:)` (`Failure == Error`).
    public func tryCatch<P: Publisher>(_ handler: @escaping (Failure) throws -> P)
    -> Publishers.TryCatch<Self, P> where P.Output == Output {
        .init(upstream: self, handler: handler)
    }
}
