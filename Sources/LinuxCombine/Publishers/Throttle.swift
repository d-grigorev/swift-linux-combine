extension Publishers {

    /// A publisher that publishes either the most-recent or first element
    /// published by the upstream publisher in a specified time interval.
    ///
    /// Official API: `struct Throttle<Upstream, Context>` with
    /// `init(upstream:interval:scheduler:latest:)`.
    ///
    /// Semantics (verified by the parity suite): one scheduled emission per
    /// interval window; `latest: true` replaces a window's pending value,
    /// `latest: false` keeps the first; values arriving with zero demand
    /// are dropped; completion piggybacks on (or schedules) a window.
    public struct Throttle<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let interval: Context.SchedulerTimeType.Stride
        public let scheduler: Context
        public let latest: Bool

        public init(
            upstream: Upstream,
            interval: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            latest: Bool
        ) {
            self.upstream = upstream
            self.interval = interval
            self.scheduler = scheduler
            self.latest = latest
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                interval: interval,
                scheduler: scheduler,
                latest: latest,
                downstream: subscriber
            ))
        }
    }
}

extension Publishers.Throttle {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let interval: Context.SchedulerTimeType.Stride
        private let scheduler: Context
        private let latest: Bool
        private var downstream: Downstream?
        private var status = SubscriptionStatus.awaitingSubscription
        private let downstreamLock = RecursiveLock()
        private var lastEmissionTime: Context.SchedulerTimeType?
        private var pendingInput: Input?
        private var pendingCompletion: Subscribers.Completion<Failure>?
        private var demand = Subscribers.Demand.none
        private var lastTime: Context.SchedulerTimeType

        init(
            interval: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            latest: Bool,
            downstream: Downstream
        ) {
            self.interval = interval
            self.scheduler = scheduler
            self.latest = latest
            self.downstream = downstream
            self.lastTime = scheduler.now
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = status, let downstream else {
                lock.unlock()
                subscription.cancel()
                return
            }
            lastTime = scheduler.now
            status = .subscribed(subscription)
            lock.unlock()

            subscription.request(.unlimited)

            downstreamLock.lock()
            downstream.receive(subscription: self)
            downstreamLock.unlock()
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return .none
            }
            let now = scheduler.now
            lastTime = now
            guard demand > .none else {
                lock.unlock()
                return .none
            }

            let hasScheduledOutput = (pendingInput != nil || pendingCompletion != nil)
            if hasScheduledOutput && latest {
                pendingInput = input
                lock.unlock()
            } else if !hasScheduledOutput {
                let minimumEmissionTime = lastEmissionTime.map { $0.advanced(by: interval) }
                let emissionTime = minimumEmissionTime.map { Swift.max(now, $0) } ?? now
                demand -= 1
                pendingInput = input
                lock.unlock()

                let action: () -> Void = { [weak self] in
                    self?.scheduledEmission()
                }
                if emissionTime == now {
                    scheduler.schedule(options: nil, action)
                } else {
                    scheduler.schedule(after: emissionTime, action)
                }
            } else {
                lock.unlock()
            }
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            lastTime = scheduler.now
            status = .pendingTerminal(subscription)

            let hasScheduledOutput = (pendingInput != nil || pendingCompletion != nil)
            if hasScheduledOutput && pendingCompletion == nil {
                pendingCompletion = completion
                lock.unlock()
            } else if !hasScheduledOutput {
                pendingCompletion = completion
                lock.unlock()
                scheduler.schedule(options: nil) { [weak self] in
                    self?.scheduledEmission()
                }
            } else {
                lock.unlock()
            }
        }

        private func scheduledEmission() {
            lock.lock()
            guard let downstream, isDeliveryActive else {
                lock.unlock()
                return
            }
            if pendingInput != nil && pendingCompletion == nil {
                lastEmissionTime = scheduler.now
            }
            let input = pendingInput
            let completion = pendingCompletion
            pendingInput = nil
            pendingCompletion = nil
            if completion != nil, case .pendingTerminal = status {
                status = .terminal
                // Release the downstream: a completed subscription must not
                // keep the subscriber alive (lifecycle tests).
                self.downstream = nil
                lock.unlock()
            } else {
                lock.unlock()
            }

            downstreamLock.lock()
            var newDemand = Subscribers.Demand.none
            if let input {
                newDemand = downstream.receive(input)
            }
            if let completion {
                downstream.receive(completion: completion)
            }
            downstreamLock.unlock()

            guard newDemand > 0 else { return }
            lock.lock()
            demand += newDemand
            lock.unlock()
        }

        private var isDeliveryActive: Bool {
            switch status {
            case .subscribed, .pendingTerminal:
                return true
            case .awaitingSubscription, .terminal:
                return false
            }
        }

        func request(_ demand: Subscribers.Demand) {
            guard demand > 0 else { return }
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            self.demand += demand
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            let subscription = status.subscription
            status = .terminal
            downstream = nil
            lock.unlock()
            subscription?.cancel()
        }

        var description: String { "Throttle" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }
    }
}

extension Publisher {
    /// Publishes either the most-recent or first element published by the
    /// upstream publisher in the specified time interval.
    public func throttle<Context: Scheduler>(
        for interval: Context.SchedulerTimeType.Stride,
        scheduler: Context,
        latest: Bool
    ) -> Publishers.Throttle<Self, Context> {
        .init(upstream: self, interval: interval, scheduler: scheduler, latest: latest)
    }
}
