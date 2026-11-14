extension Publisher {
    /// Specifies the scheduler on which to receive elements from the publisher.
    ///
    /// Hops element and completion delivery onto `scheduler`; the
    /// subscriber's `receive(subscription:)` is NOT affected.
    public func receive<Context: Scheduler>(
        on scheduler: Context,
        options: Context.SchedulerOptions? = nil
    ) -> Publishers.ReceiveOn<Self, Context> {
        .init(upstream: self, scheduler: scheduler, options: options)
    }
}

extension Publishers {

    /// A publisher that delivers elements to its downstream subscriber on a
    /// specific scheduler.
    ///
    /// Official API verified against Apple DocC: `struct
    /// ReceiveOn<Upstream, Context>` with `init(upstream:scheduler:options:)`
    /// and `upstream`/`scheduler`/`options` properties.
    ///
    /// Semantics (verified by the parity suite): element and completion
    /// deliveries hop onto the scheduler; the subscription grant does NOT
    /// (per official docs); values return `.none` and any additional
    /// downstream demand is re-requested on the upstream *from inside* the
    /// scheduled delivery; `request`/`cancel` pass through directly.
    public struct ReceiveOn<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The scheduler the publisher is to use for element delivery.
        public let scheduler: Context

        /// Scheduler options that customize the delivery of elements.
        public let options: Context.SchedulerOptions?

        public init(upstream: Upstream, scheduler: Context, options: Context.SchedulerOptions?) {
            self.upstream = upstream
            self.scheduler = scheduler
            self.options = options
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(scheduler: scheduler, options: options, downstream: subscriber)
            upstream.subscribe(inner)
        }
    }
}

extension Publishers.ReceiveOn {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private let scheduler: Context
        private let options: Context.SchedulerOptions?
        private var state = SubscriptionStatus.awaitingSubscription
        private let downstreamLock = RecursiveLock()

        init(scheduler: Context, options: Context.SchedulerOptions?, downstream: Downstream) {
            self.downstream = downstream
            self.scheduler = scheduler
            self.options = options
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            lock.unlock()
            // NOTE: the subscription grant does not hop onto the scheduler.
            downstreamLock.lock()
            downstream.receive(subscription: self)
            downstreamLock.unlock()
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledReceive(input)
            }
            return .none
        }

        private func scheduledReceive(_ input: Input) {
            lock.lock()
            guard state.subscription != nil else {
                lock.unlock()
                return
            }
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(input)
            downstreamLock.unlock()
            guard newDemand != .none else { return }
            lock.lock()
            let subscription = state.subscription
            lock.unlock()
            subscription?.request(newDemand)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .pendingTerminal(subscription)
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledReceive(completion: completion)
            }
        }

        private func scheduledReceive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            state = .terminal
            lock.unlock()
            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "ReceiveOn" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}
