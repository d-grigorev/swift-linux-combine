extension Publishers {

    /// A publisher that measures and emits the time interval between events
    /// received from an upstream publisher.
    ///
    /// Official API: `struct MeasureInterval<Upstream, Context>` with
    /// `init(upstream:scheduler:)`; `Output == Context.SchedulerTimeType.Stride`.
    ///
    /// Semantics (verified by the parity suite): the reference time is
    /// captured at subscription; each element emits
    /// `previous.distance(to: now)` and forwards any demand the downstream
    /// returns.
    public struct MeasureInterval<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Context.SchedulerTimeType.Stride
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let scheduler: Context

        public init(upstream: Upstream, scheduler: Context) {
            self.upstream = upstream
            self.scheduler = scheduler
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(scheduler: scheduler, downstream: subscriber))
        }
    }
}

extension Publishers.MeasureInterval {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Context.SchedulerTimeType.Stride,
          Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private let scheduler: Context
        private var state = SubscriptionStatus.awaitingSubscription
        private var last: Context.SchedulerTimeType?

        init(scheduler: Context, downstream: Downstream) {
            self.scheduler = scheduler
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            last = scheduler.now
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case let .subscribed(subscription) = state, let previousTime = last else {
                lock.unlock()
                return .none
            }
            let now = scheduler.now
            last = now
            lock.unlock()
            let newDemand = downstream.receive(previousTime.distance(to: now))
            if newDemand > 0 {
                subscription.request(newDemand)
            }
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            state = .terminal
            last = nil
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
            subscription.request(demand)
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            last = nil
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "MeasureInterval" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Measures and emits the time interval between events received from
    /// an upstream publisher.
    ///
    /// (The official signature accepts `options` but the operator does not
    /// use them — mirrored faithfully.)
    public func measureInterval<Context: Scheduler>(
        using scheduler: Context,
        options: Context.SchedulerOptions? = nil
    ) -> Publishers.MeasureInterval<Self, Context> {
        .init(upstream: self, scheduler: scheduler)
    }
}
