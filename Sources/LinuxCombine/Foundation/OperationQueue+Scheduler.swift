import Foundation
import Dispatch

// Note: no Apple Combine on this platform, so the conformance lives directly
// on `OperationQueue` (official shape: nested SchedulerTimeType /
// SchedulerOptions + witnesses on OperationQueue itself — mirroring the
// RunLoop conformance).

extension OperationQueue: Scheduler {

    /// The scheduler time type that the operation queue uses.
    public struct SchedulerTimeType: Strideable, Codable, Hashable {
        /// The date represented by this type.
        public var date: Date

        /// Initializes an operation queue scheduler time with the given date.
        public init(_ date: Date) {
            self.date = date
        }

        public func distance(to other: SchedulerTimeType) -> Stride {
            let absoluteSelf = date.timeIntervalSinceReferenceDate
            let absoluteOther = other.date.timeIntervalSinceReferenceDate
            return Stride(absoluteSelf.distance(to: absoluteOther))
        }

        public func advanced(by value: Stride) -> SchedulerTimeType {
            SchedulerTimeType(date + value.magnitude)
        }

        /// The interval by which operation queue times advance.
        public struct Stride: SchedulerTimeIntervalConvertible,
                              Comparable,
                              SignedNumeric,
                              ExpressibleByFloatLiteral,
                              Codable {
            public typealias FloatLiteralType = TimeInterval
            public typealias IntegerLiteralType = TimeInterval
            public typealias Magnitude = TimeInterval

            /// The value of this time interval in seconds.
            public var magnitude: TimeInterval

            /// The value of this time interval in seconds.
            public var timeInterval: TimeInterval { magnitude }

            public init(integerLiteral value: TimeInterval) {
                magnitude = value
            }

            public init(floatLiteral value: TimeInterval) {
                magnitude = value
            }

            public init(_ timeInterval: TimeInterval) {
                magnitude = timeInterval
            }

            public init?<Source: BinaryInteger>(exactly source: Source) {
                guard let value = TimeInterval(exactly: source) else { return nil }
                magnitude = value
            }

            public static func < (lhs: Stride, rhs: Stride) -> Bool {
                lhs.magnitude < rhs.magnitude
            }

            public static func * (lhs: Stride, rhs: Stride) -> Stride {
                Stride(lhs.magnitude * rhs.magnitude)
            }

            public static func + (lhs: Stride, rhs: Stride) -> Stride {
                Stride(lhs.magnitude + rhs.magnitude)
            }

            public static func - (lhs: Stride, rhs: Stride) -> Stride {
                Stride(lhs.magnitude - rhs.magnitude)
            }

            public static func *= (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude *= rhs.magnitude
            }

            public static func += (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude += rhs.magnitude
            }

            public static func -= (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude -= rhs.magnitude
            }

            public static func seconds(_ value: Int) -> Stride {
                Stride(TimeInterval(value))
            }

            public static func seconds(_ value: Double) -> Stride {
                Stride(TimeInterval(value))
            }

            public static func milliseconds(_ value: Int) -> Stride {
                Stride(TimeInterval(value) / 1_000)
            }

            public static func microseconds(_ value: Int) -> Stride {
                Stride(TimeInterval(value) / 1_000_000)
            }

            public static func nanoseconds(_ value: Int) -> Stride {
                Stride(TimeInterval(value) / 1_000_000_000)
            }
        }
    }

    /// A set of options that affect the operation of the operation queue
    /// scheduler.
    public struct SchedulerOptions {}

    public func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        // Wrapped for BlockOperation's @Sendable parameter; the official
        // signature keeps the plain `() -> Void`.
        let work: @Sendable () -> Void = { action() }
        addOperation(BlockOperation(block: work))
    }

    public func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) {
        // The operation is enqueued immediately but only *becomes ready* at
        // the target date (KVO-gated `isReady`), so the queue honors the
        // delay (tolerance is accepted but not independently honored —
        // mirroring the reference implementation).
        addOperation(DelayReadyOperation.once(action: action, after: date))
    }

    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        let context = DelayReadyOperation.CancellationContext()
        let operation = DelayReadyOperation.repeating(
            action: action,
            after: date,
            queue: self,
            interval: interval,
            context: context
        )
        context.operation = operation
        addOperation(operation)
        return AnyCancellable(context)
    }

    public var now: SchedulerTimeType {
        .init(Date())
    }

    public var minimumTolerance: SchedulerTimeType.Stride {
        .init(0)
    }

    /// An `Operation` that reports itself not-ready until a dispatch timer
    /// flips it (via KVO, which `OperationQueue` observes), then runs its
    /// action. Repeating schedules chain themselves: after each run the
    /// next operation is enqueued and armed `interval` later.
    private final class DelayReadyOperation: Operation, Cancellable, @unchecked Sendable {

        fileprivate final class CancellationContext: Cancellable {
            let lock = Lock()
            weak var operation: DelayReadyOperation?

            func cancel() {
                lock.lock()
                guard let operation = self.operation else {
                    lock.unlock()
                    return
                }
                lock.unlock()
                operation.action = nil
                operation.queue = nil
                operation.context = nil
                operation.cancel()
            }
        }

        private static let readySchedulingQueue =
            DispatchQueue(label: "DelayReadyOperation")

        private let readyFromAfterLock = Lock()
        private var action: (() -> Void)?
        private var readyFromAfter = false
        private var queue: OperationQueue?
        private let interval: SchedulerTimeType.Stride
        private var context: CancellationContext?

        init(
            action: @escaping () -> Void,
            queue: OperationQueue?,
            interval: SchedulerTimeType.Stride,
            context: CancellationContext?
        ) {
            self.action = action
            self.queue = queue
            self.interval = interval
            self.context = context
            super.init()
        }

        static func once(action: @escaping () -> Void, after: SchedulerTimeType)
            -> DelayReadyOperation {
            let operation = DelayReadyOperation(
                action: action,
                queue: nil,
                interval: 0,
                context: nil
            )
            operation.becomeReady(after: after.date.timeIntervalSinceNow, from: .now())
            return operation
        }

        static func repeating(
            action: @escaping () -> Void,
            after: SchedulerTimeType,
            queue: OperationQueue,
            interval: SchedulerTimeType.Stride,
            context: CancellationContext
        ) -> DelayReadyOperation {
            let operation = DelayReadyOperation(
                action: action,
                queue: queue,
                interval: interval,
                context: context
            )
            operation.becomeReady(after: after.date.timeIntervalSinceNow, from: .now())
            return operation
        }

        override func main() {
            guard let action = self.action else { return }
            self.action = nil
            action()

            guard let queue = self.queue, let context = self.context else {
                self.queue = nil
                self.context = nil
                return
            }
            self.queue = nil
            self.context = nil

            context.lock.lock()
            if context.operation == nil {
                context.lock.unlock()
                return
            }
            let nextOperation = DelayReadyOperation(
                action: action,
                queue: queue,
                interval: interval,
                context: context
            )
            context.operation = nextOperation
            queue.addOperation(nextOperation)
            context.lock.unlock()
            nextOperation.becomeReady(after: interval.timeInterval, from: .now())
        }

        private func becomeReady(after delay: TimeInterval, from time: DispatchTime) {
            DelayReadyOperation.readySchedulingQueue
                .asyncAfter(deadline: time + delay) { [weak self] in
                    self?.becomeReady()
                }
        }

        private func becomeReady() {
            // KVO notifications let the queue re-evaluate readiness.
            willChangeValue(forKey: "isReady")
            readyFromAfterLock.lock()
            readyFromAfter = true
            readyFromAfterLock.unlock()
            didChangeValue(forKey: "isReady")
        }

        override var isReady: Bool {
            guard super.isReady else { return false }
            readyFromAfterLock.lock()
            defer { readyFromAfterLock.unlock() }
            return readyFromAfter
        }
    }
}
