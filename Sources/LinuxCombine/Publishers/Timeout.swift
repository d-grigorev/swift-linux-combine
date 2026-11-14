extension Publishers {

    /// A publisher that terminates publishing if the upstream publisher
    /// exceeds the specified time interval without producing an element.
    ///
    /// Official API: `struct Timeout<Upstream, Context>` with
    /// `init(upstream:interval:scheduler:options:customError:)`.
    ///
    /// Semantics (verified by the parity suite): the clock starts on
    /// subscription (and re-arms on the first request); every element
    /// re-arms it; on timeout the upstream is cancelled and the downstream
    /// receives `.finished` (or `customError()`'s failure); elements hop
    /// through the scheduler.
    public struct Timeout<Upstream: Publisher, Context: Scheduler>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        public let upstream: Upstream
        public let interval: Context.SchedulerTimeType.Stride
        public let scheduler: Context
        public let options: Context.SchedulerOptions?
        public let customError: (() -> Upstream.Failure)?

        public init(
            upstream: Upstream,
            interval: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            options: Context.SchedulerOptions?,
            customError: (() -> Publishers.Timeout<Upstream, Context>.Failure)?
        ) {
            self.upstream = upstream
            self.interval = interval
            self.scheduler = scheduler
            self.options = options
            self.customError = customError
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                interval: interval,
                scheduler: scheduler,
                options: options,
                customError: customError
            ))
        }
    }
}

extension Publishers.Timeout {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstreamLock = RecursiveLock()
        private let downstream: Downstream
        private let interval: Context.SchedulerTimeType.Stride
        private let scheduler: Context
        private let options: Context.SchedulerOptions?
        private let customError: (() -> Upstream.Failure)?
        private var state = SubscriptionStatus.awaitingSubscription
        private var didTimeout = false
        private var timer: (any Cancellable)?
        private var initialDemand = false

        init(
            downstream: Downstream,
            interval: Context.SchedulerTimeType.Stride,
            scheduler: Context,
            options: Context.SchedulerOptions?,
            customError: (() -> Upstream.Failure)?
        ) {
            self.downstream = downstream
            self.interval = interval
            self.scheduler = scheduler
            self.options = options
            self.customError = customError
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            timer = timeoutClock()
            lock.unlock()
            downstreamLock.lock()
            downstream.receive(subscription: self)
            downstreamLock.unlock()
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard !didTimeout, case .subscribed = state else {
                lock.unlock()
                return .none
            }
            timer?.cancel()
            didTimeout = false
            timer = timeoutClock()
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledReceive(input)
            }
            return .none
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            timer?.cancel()
            lock.unlock()
            scheduler.schedule(options: options) {
                self.scheduledReceive(completion: completion)
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            if !initialDemand {
                timer = timeoutClock()
                initialDemand = true
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
            timer?.cancel()
            subscription.cancel()
        }

        var description: String { "Timeout" }
        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }
        var playgroundDescription: Any { description }

        private func timedOut() {
            lock.lock()
            guard !didTimeout, case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            didTimeout = true
            state = .terminal
            lock.unlock()
            subscription.cancel()
            downstreamLock.lock()
            downstream.receive(
                completion: customError.map { .failure($0()) } ?? .finished
            )
            downstreamLock.unlock()
        }

        private func timeoutClock() -> any Cancellable {
            let cancellable = scheduler.schedule(
                after: scheduler.now.advanced(by: interval),
                interval: interval,
                tolerance: scheduler.minimumTolerance,
                options: options,
                timedOut
            )
            return AnyCancellable(cancellable.cancel)
        }

        private func scheduledReceive(_ input: Input) {
            lock.lock()
            guard !didTimeout, case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(input)
            downstreamLock.unlock()
            if newDemand != .none {
                subscription.request(newDemand)
            }
        }

        private func scheduledReceive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            guard case .subscribed = state else {
                lock.unlock()
                return
            }
            state = .terminal
            lock.unlock()
            downstreamLock.lock()
            downstream.receive(completion: completion)
            downstreamLock.unlock()
        }
    }
}

extension Publisher {
    /// Terminates publishing if the upstream publisher exceeds the
    /// specified time interval without producing an element.
    public func timeout<Context: Scheduler>(
        _ interval: Context.SchedulerTimeType.Stride,
        scheduler: Context,
        options: Context.SchedulerOptions? = nil,
        customError: (() -> Failure)? = nil
    ) -> Publishers.Timeout<Self, Context> {
        .init(
            upstream: self,
            interval: interval,
            scheduler: scheduler,
            options: options,
            customError: customError
        )
    }
}
