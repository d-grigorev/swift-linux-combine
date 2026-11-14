import Foundation

extension Timer {

    /// Returns a publisher that repeatedly emits the current date on the
    /// given interval.
    ///
    /// Official API verified against Apple DocC:
    /// `static func publish(every:tolerance:on:in:options:) -> Timer.TimerPublisher`.
    ///
    /// Quirk (documented in docs/QUIRKS.md): the `tolerance` parameter is
    /// **ignored** — an Apple Combine bug faithfully preserved here.
    public static func publish(
        every interval: TimeInterval,
        tolerance: TimeInterval? = nil,
        on runLoop: RunLoop,
        in mode: RunLoop.Mode,
        options: RunLoop.SchedulerOptions? = nil
    ) -> TimerPublisher {
        TimerPublisher(interval: interval, runLoop: runLoop, mode: mode, options: options)
    }

    /// A publisher that repeatedly emits the current date on a given interval.
    ///
    /// Official API: `final class TimerPublisher` conforming to
    /// `ConnectablePublisher` with `init(interval:tolerance:runLoop:mode:options:)`
    /// and `interval`/`tolerance`/`runLoop`/`mode`/`options` properties.
    public final class TimerPublisher: ConnectablePublisher {
        public typealias Output = Date
        public typealias Failure = Never

        public let interval: TimeInterval
        public let tolerance: TimeInterval?
        public let runLoop: RunLoop
        public let mode: RunLoop.Mode
        public let options: RunLoop.SchedulerOptions?

        private var sides: [CombineIdentifier: Side] = [:]
        private let lock = Lock()

        public init(
            interval: TimeInterval,
            tolerance: TimeInterval? = nil,
            runLoop: RunLoop,
            mode: RunLoop.Mode,
            options: RunLoop.SchedulerOptions? = nil
        ) {
            self.interval = interval
            self.tolerance = tolerance
            self.runLoop = runLoop
            self.mode = mode
            self.options = options
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(parent: self, downstream: subscriber)
            lock.lock()
            sides[inner.combineIdentifier] = Side(inner)
            lock.unlock()
            subscriber.receive(subscription: inner)
        }

        public func connect() -> any Cancellable {
            let timer = Timer(
                fire: Date() + interval,
                interval: interval,
                repeats: true,
                block: { [self] in fire($0) }
            )
            timer.tolerance = tolerance ?? 0
            runLoop.add(timer, forMode: mode)
            return CancellableTimer(timer: timer, publisher: self)
        }

        private func fire(_ timer: Timer) {
            lock.lock()
            let sides = self.sides
            lock.unlock()
            let now = Date()
            for side in sides.values {
                side.send(now)
            }
        }

        fileprivate func disconnect(_ innerID: CombineIdentifier) {
            lock.lock()
            sides[innerID] = nil
            lock.unlock()
        }

        fileprivate func disconnectAll() {
            lock.lock()
            sides = [:]
            lock.unlock()
        }

        fileprivate struct Side {
            let send: (Date) -> Void

            init<S: Subscriber>(_ inner: Inner<S>)
            where S.Input == Date, S.Failure == Never {
                send = inner.send
            }
        }

        private struct CancellableTimer: Cancellable {
            let timer: Timer
            let publisher: TimerPublisher

            func cancel() {
                publisher.disconnectAll()
                timer.invalidate()
            }
        }

        fileprivate final class Inner<S: Subscriber>: Subscription
        where S.Input == Date, S.Failure == Never {
            private var downstream: S?
            private var pending = Subscribers.Demand.none
            private weak var parent: TimerPublisher?
            private let lock = Lock()

            init(parent: TimerPublisher, downstream: S) {
                self.parent = parent
                self.downstream = downstream
            }

            func send(_ date: Date) {
                lock.lock()
                guard let downstream, pending != .none else {
                    lock.unlock()
                    return
                }
                pending -= 1
                lock.unlock()
                let newDemand = downstream.receive(date)
                guard newDemand != .none else { return }
                lock.lock()
                pending += newDemand
                lock.unlock()
            }

            func request(_ demand: Subscribers.Demand) {
                lock.lock()
                guard downstream != nil else {
                    lock.unlock()
                    return
                }
                pending += demand
                lock.unlock()
            }

            func cancel() {
                lock.lock()
                guard downstream != nil else {
                    lock.unlock()
                    return
                }
                downstream = nil
                let parent = self.parent
                lock.unlock()
                parent?.disconnect(combineIdentifier)
            }
        }
    }
}
