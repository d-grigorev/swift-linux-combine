extension Publishers {

    /// A publisher that publishes elements only after a specified time
    /// interval elapses between events.
    ///
    /// Official API: `struct Debounce<Upstream, Context>` with
    /// `init(upstream:dueTime:scheduler:options:)`.
    ///
    /// Semantics (verified by the parity suite): values restart a
    /// generation-tagged timer; only the newest generation's timer may
    /// deliver; the subscription is granted downstream *before* the
    /// `.unlimited` upstream pull; emission is demand-gated at due time
    /// (a due event without demand is dropped); completion is scheduled
    /// on the scheduler after cancelling timers.
    public struct Debounce<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let dueTime: Context.SchedulerTimeType.Stride
        public let scheduler: Context
        public let options: Context.SchedulerOptions?

        public init(
            upstream: Upstream,
            dueTime: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            options: Context.SchedulerOptions?
        ) {
            self.upstream = upstream
            self.dueTime = dueTime
            self.scheduler = scheduler
            self.options = options
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                dueTime: dueTime,
                scheduler: scheduler,
                options: options
            ))
        }
    }
}

extension Publishers.Debounce {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private typealias Generation = UInt64

        private enum CancellerState {
            case pending
            case active(any Cancellable)

            fileprivate func cancel() {
                if case let .active(cancellable) = self {
                    cancellable.cancel()
                }
            }
        }

        private let lock = Lock()
        private let downstreamLock = RecursiveLock()
        private let downstream: Downstream
        private let dueTime: Context.SchedulerTimeType.Stride
        private let scheduler: Context
        private let options: Context.SchedulerOptions?
        private var state = SubscriptionStatus.awaitingSubscription
        private var currentCancellers: [Generation: CancellerState] = [:]
        private var currentValue: Output?
        private var currentGeneration: Generation = 0
        private var downstreamDemand = Subscribers.Demand.none

        init(
            downstream: Downstream,
            dueTime: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            options: Context.SchedulerOptions?
        ) {
            self.downstream = downstream
            self.dueTime = dueTime
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
            downstreamLock.lock()
            downstream.receive(subscription: self)
            downstreamLock.unlock()
            subscription.request(.unlimited)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return .none
            }
            currentGeneration += 1
            let generation = currentGeneration
            currentValue = input
            let due = scheduler.now.advanced(by: dueTime)
            let previous = currentCancellers
            currentCancellers = [:]
            currentCancellers[generation] = .pending
            lock.unlock()

            let newCanceller = scheduler.schedule(
                after: due,
                interval: dueTime,
                tolerance: scheduler.minimumTolerance,
                options: options
            ) {
                self.due(generation: generation)
            }
            lock.lock()
            currentCancellers[generation] = .active(newCanceller)
            lock.unlock()
            for canceller in previous.values {
                canceller.cancel()
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
            let previous = currentCancellers
            currentCancellers = [:]
            lock.unlock()
            for canceller in previous.values {
                canceller.cancel()
            }
            scheduler.schedule {
                self.downstreamLock.lock()
                self.downstream.receive(completion: completion)
                self.downstreamLock.unlock()
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            downstreamDemand += demand
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            let previous = currentCancellers
            currentCancellers = [:]
            lock.unlock()
            for canceller in previous.values {
                canceller.cancel()
            }
            subscription.cancel()
        }

        var description: String { "Debounce" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream),
                ("downstreamDemand", downstreamDemand),
                ("currentValue", currentValue as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }

        private func due(generation: Generation) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            // A newer value arrived in this window: only the newest
            // generation may deliver.
            guard generation == currentGeneration, let value = currentValue else {
                let canceller = currentCancellers[generation]
                lock.unlock()
                canceller?.cancel()
                return
            }
            // Take whatever state the canceller is in: with the immediate
            // scheduler the timer fires synchronously while the entry is
            // still `.pending` (reference behavior — cancel below is a
            // no-op for pending entries).
            guard let canceller = currentCancellers.removeValue(forKey: generation) else {
                lock.unlock()
                return
            }

            let hasAnyDemand = downstreamDemand != .none
            if hasAnyDemand {
                downstreamDemand -= 1
            }
            lock.unlock()
            canceller.cancel()
            guard hasAnyDemand else { return }

            downstreamLock.lock()
            let newDemand = downstream.receive(value)
            downstreamLock.unlock()
            guard newDemand != .none else { return }
            lock.lock()
            downstreamDemand += newDemand
            lock.unlock()
        }
    }
}

extension Publisher {
    /// Publishes elements only after a specified time interval elapses
    /// between events.
    public func debounce<Context: Scheduler>(
        for dueTime: Context.SchedulerTimeType.Stride,
        scheduler: Context,
        options: Context.SchedulerOptions? = nil
    ) -> Publishers.Debounce<Self, Context> {
        .init(upstream: self, dueTime: dueTime, scheduler: scheduler, options: options)
    }
}
