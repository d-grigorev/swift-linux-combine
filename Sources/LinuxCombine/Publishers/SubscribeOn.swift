extension Publisher {
    /// Specifies the scheduler on which to perform subscribe, cancel, and
    /// request operations.
    public func subscribe<Context: Scheduler>(
        on scheduler: Context,
        options: Context.SchedulerOptions? = nil
    ) -> Publishers.SubscribeOn<Self, Context> {
        .init(upstream: self, scheduler: scheduler, options: options)
    }
}

extension Publishers {

    /// A publisher that receives elements from an upstream publisher on a
    /// specific scheduler.
    ///
    /// Official API verified against Apple DocC: `struct
    /// SubscribeOn<Upstream, Context>` with `init(upstream:scheduler:options:)`
    /// and `upstream`/`scheduler`/`options` properties.
    ///
    /// Semantics (verified by the parity suite): the *upstream subscription
    /// itself* is created on the scheduler; `request` and `cancel` are
    /// scheduled onto it (serialized by a dedicated lock); element and
    /// completion delivery pass through directly.
    public struct SubscribeOn<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The scheduler the publisher should use to receive elements.
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
            scheduler.schedule(options: options) {
                self.upstream.subscribe(inner)
            }
        }
    }
}

extension Publishers.SubscribeOn {

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
        private let upstreamLock = Lock()

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
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledRequest(demand, subscription: subscription)
            }
        }

        private func scheduledRequest(_ demand: Subscribers.Demand, subscription: any Subscription) {
            upstreamLock.lock()
            subscription.request(demand)
            upstreamLock.unlock()
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledCancel(subscription)
            }
        }

        private func scheduledCancel(_ subscription: any Subscription) {
            upstreamLock.lock()
            subscription.cancel()
            upstreamLock.unlock()
        }

        var description: String { "SubscribeOn" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}
