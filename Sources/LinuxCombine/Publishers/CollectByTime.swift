extension Publishers {

    /// A strategy for grouping elements emitted by `CollectByTime`.
    ///
    /// Official API: cases `byTime(_:_:)` and `byTimeOrCount(_:_:_:)`
    /// (verified against Apple DocC).
    public enum TimeGroupingStrategy<Context: Scheduler> {
        /// Emit a group whenever the time interval elapses.
        case byTime(Context, Context.SchedulerTimeType.Stride)
        /// Emit a group when either the count is reached or the interval
        /// elapses — whichever comes first.
        case byTimeOrCount(Context, Context.SchedulerTimeType.Stride, Int)
    }

    /// A publisher that collects elements into groups based on time or
    /// count criteria.
    ///
    /// Official API: `struct CollectByTime<Upstream, Context>`.
    ///
    /// Semantics note: no upstream oracle exists for this operator
    /// (OpenCombine doesn't implement it — docs/BACKLOG.md group 1), so
    /// these are own-test-pinned choices, verified by
    /// `CombineTests/CollectByTimeTests`: upstream is pulled `.unlimited`;
    /// a window with no elements emits nothing; emission is demand-gated
    /// (a due group is held until requested); upstream completion flushes
    /// the partial group with `.finished`, demand-gated like `Collect`.
    public struct CollectByTime<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = [Upstream.Output]
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let strategy: Publishers.TimeGroupingStrategy<Context>

        public init(upstream: Upstream, strategy: Publishers.TimeGroupingStrategy<Context>) {
            self.upstream = upstream
            self.strategy = strategy
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(strategy: strategy, downstream: subscriber))
        }
    }
}

extension Publisher {
    /// Collects elements into groups based on the given time/count
    /// grouping strategy.
    public func collect<Context: Scheduler>(
        _ strategy: Publishers.TimeGroupingStrategy<Context>,
        options: Context.SchedulerOptions? = nil
    ) -> Publishers.CollectByTime<Self, Context> {
        .init(upstream: self, strategy: strategy)
    }
}

extension Publishers.CollectByTime {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == [Upstream.Output], Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private let strategy: Publishers.TimeGroupingStrategy<Context>
        private var status = SubscriptionStatus.awaitingSubscription
        private var buffer: [Input] = []
        private var demand = Subscribers.Demand.none
        private var windowTimer: (any Cancellable)?
        private var windowPending = false
        private var finishedPending = false
        private var countLimit: Int?

        init(strategy: Publishers.TimeGroupingStrategy<Context>, downstream: Downstream) {
            self.strategy = strategy
            self.downstream = downstream
            switch strategy {
            case .byTimeOrCount(_, _, let count):
                countLimit = Swift.max(1, count)
            case .byTime:
                countLimit = nil
            }
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = status else {
                lock.unlock()
                subscription.cancel()
                return
            }
            status = .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
            subscription.request(.unlimited)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return .none
            }
            buffer.append(input)
            if let limit = countLimit, buffer.count >= limit {
                // Count reached: flush now (or hold until requested).
                lock.unlock()
                flushWindow()
                return .none
            }
            let needsTimer = (windowTimer == nil && !windowPending)
            lock.unlock()
            if needsTimer {
                armWindowTimer()
            }
            return .none
        }

        private func armWindowTimer() {
            let time: Context.SchedulerTimeType.Stride
            switch strategy {
            case .byTime(_, let interval), .byTimeOrCount(_, let interval, _):
                time = interval
            }
            let timer = scheduler.schedule(
                after: scheduler.now.advanced(by: time),
                interval: time,
                tolerance: .nanoseconds(0),
                options: nil
            ) { [weak self] in
                self?.windowElapsed()
            }
            lock.lock()
            if status.subscription != nil, windowTimer == nil, !finishedPending {
                windowTimer = timer
            } else {
                lock.unlock()
                timer.cancel()
                return
            }
            lock.unlock()
        }

        private var scheduler: Context {
            switch strategy {
            case .byTime(let context, _), .byTimeOrCount(let context, _, _):
                return context
            }
        }

        private func windowElapsed() {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return
            }
            windowTimer = nil
            lock.unlock()
            flushWindow()
        }

        /// Emits the current group if there is one (demand-gated; held
        /// until a request arrives otherwise).
        private func flushWindow() {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return
            }
            guard !buffer.isEmpty else {
                windowPending = false
                lock.unlock()
                return
            }
            guard demand > 0 else {
                windowPending = true
                lock.unlock()
                return
            }
            let group = buffer
            buffer = []
            windowPending = false
            demand -= 1
            lock.unlock()
            let newDemand = downstream.receive(group)
            guard newDemand > 0 else { return }
            lock.lock()
            demand += newDemand
            lock.unlock()
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = status, !finishedPending else {
                lock.unlock()
                return
            }
            windowTimer?.cancel()
            windowTimer = nil
            switch completion {
            case .failure:
                buffer = []
                status = .terminal
                lock.unlock()
                downstream.receive(completion: completion)
            case .finished:
                if demand > 0, !buffer.isEmpty {
                    let group = buffer
                    buffer = []
                    status = .terminal
                    lock.unlock()
                    _ = downstream.receive(group)
                    downstream.receive(completion: .finished)
                } else if demand > 0 {
                    status = .terminal
                    lock.unlock()
                    downstream.receive(completion: .finished)
                } else {
                    finishedPending = true
                    lock.unlock()
                }
            }
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            self.demand += demand
            if finishedPending, self.demand > 0 {
                let group = buffer
                buffer = []
                status = .terminal
                lock.unlock()
                if !group.isEmpty {
                    _ = downstream.receive(group)
                }
                downstream.receive(completion: .finished)
                return
            }
            if windowPending, self.demand > 0 {
                lock.unlock()
                flushWindow()
                return
            }
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            let timer = windowTimer
            windowTimer = nil
            lock.unlock()
            timer?.cancel()
            subscription.cancel()
        }

        var description: String { "CollectByTime" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream),
                ("buffer", buffer),
                ("demand", demand),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
