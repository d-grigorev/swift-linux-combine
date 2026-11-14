extension Publishers {

    /// A publisher that performs the specified closures when publisher
    /// events occur.
    ///
    /// Official API: `struct HandleEvents<Upstream>` with the six-parameter
    /// init.
    ///
    /// Semantics (verified by the parity suite): `receiveRequest` fires both
    /// for downstream `request(_:)` calls and for demands returned
    /// synchronously by `receive(_:)`; on terminal (completion or cancel)
    /// all hooks are released.
    public struct HandleEvents<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// A closure that executes when the publisher receives the
        /// subscription from the upstream publisher.
        public var receiveSubscription: ((any Subscription) -> Void)?

        /// A closure that executes when the publisher receives a value from
        /// the upstream publisher.
        public var receiveOutput: ((Upstream.Output) -> Void)?

        /// A closure that executes when the publisher receives the
        /// completion from the upstream publisher.
        public var receiveCompletion: ((Subscribers.Completion<Upstream.Failure>) -> Void)?

        /// A closure that executes when the downstream receiver cancels
        /// publishing.
        public var receiveCancel: (() -> Void)?

        /// A closure that executes when the publisher receives a request for
        /// more elements.
        public var receiveRequest: ((Subscribers.Demand) -> Void)?

        public init(
            upstream: Upstream,
            receiveSubscription: ((any Subscription) -> Void)? = nil,
            receiveOutput: ((Output) -> Void)? = nil,
            receiveCompletion: ((Subscribers.Completion<Failure>) -> Void)? = nil,
            receiveCancel: (() -> Void)? = nil,
            receiveRequest: ((Subscribers.Demand) -> Void)? = nil
        ) {
            self.upstream = upstream
            self.receiveSubscription = receiveSubscription
            self.receiveOutput = receiveOutput
            self.receiveCompletion = receiveCompletion
            self.receiveCancel = receiveCancel
            self.receiveRequest = receiveRequest
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(self, downstream: subscriber))
        }
    }
}

extension Publisher {
    /// Performs the specified closures when publisher events occur.
    public func handleEvents(
        receiveSubscription: ((any Subscription) -> Void)? = nil,
        receiveOutput: ((Output) -> Void)? = nil,
        receiveCompletion: ((Subscribers.Completion<Failure>) -> Void)? = nil,
        receiveCancel: (() -> Void)? = nil,
        receiveRequest: ((Subscribers.Demand) -> Void)? = nil
    ) -> Publishers.HandleEvents<Self> {
        .init(
            upstream: self,
            receiveSubscription: receiveSubscription,
            receiveOutput: receiveOutput,
            receiveCompletion: receiveCompletion,
            receiveCancel: receiveCancel,
            receiveRequest: receiveRequest
        )
    }
}

extension Publishers.HandleEvents {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private var status = SubscriptionStatus.awaitingSubscription
        fileprivate var receiveSubscription: ((any Subscription) -> Void)?
        fileprivate var receiveOutput: ((Upstream.Output) -> Void)?
        fileprivate var receiveCompletion: ((Subscribers.Completion<Upstream.Failure>) -> Void)?
        fileprivate var receiveCancel: (() -> Void)?
        fileprivate var receiveRequest: ((Subscribers.Demand) -> Void)?
        private let downstream: Downstream

        init(_ events: Publishers.HandleEvents<Upstream>, downstream: Downstream) {
            self.receiveSubscription = events.receiveSubscription
            self.receiveOutput = events.receiveOutput
            self.receiveCompletion = events.receiveCompletion
            self.receiveCancel = events.receiveCancel
            self.receiveRequest = events.receiveRequest
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            if let receiveSubscription = self.receiveSubscription {
                lock.unlock()
                receiveSubscription(subscription)
                lock.lock()
            }
            guard case .awaitingSubscription = status else {
                lock.unlock()
                subscription.cancel()
                return
            }
            status = .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if let receiveOutput = self.receiveOutput {
                lock.unlock()
                receiveOutput(input)
            } else {
                lock.unlock()
            }
            let newDemand = downstream.receive(input)
            if newDemand == .none {
                return newDemand
            }
            lock.lock()
            if let receiveRequest = self.receiveRequest {
                lock.unlock()
                receiveRequest(newDemand)
            } else {
                lock.unlock()
            }
            return newDemand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            if let receiveCompletion = self.receiveCompletion {
                lock.unlock()
                receiveCompletion(completion)
                lock.lock()
            }
            lockedTerminate()
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            if let receiveRequest = self.receiveRequest {
                lock.unlock()
                receiveRequest(demand)
                lock.lock()
            }
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            if let receiveCancel = self.receiveCancel {
                lock.unlock()
                receiveCancel()
                lock.lock()
            }
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            lockedTerminate()
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "HandleEvents" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }

        private func lockedTerminate() {
            receiveSubscription = nil
            receiveOutput = nil
            receiveCompletion = nil
            receiveCancel = nil
            receiveRequest = nil
            status = .terminal
        }
    }
}
