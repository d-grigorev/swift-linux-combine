import Foundation

// Note: no Apple Combine on this platform, so the conformance lives
// directly on `RunLoop` (official shape: nested SchedulerTimeType /
// SchedulerOptions + witnesses on RunLoop itself).

extension RunLoop: Scheduler {

    /// The scheduler time type that the run loop uses.
    public struct SchedulerTimeType: Strideable, Codable, Hashable {
        /// The date represented by this type.
        public var date: Date

        /// Initializes a run loop scheduler time with the given date.
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

        /// The interval by which run loop times advance.
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

    /// A set of options that affect the operation of the run loop scheduler.
    public struct SchedulerOptions {}

    public func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        // Wrapped for RunLoop's @Sendable parameter; the official signature
        // keeps the plain `() -> Void`.
        let work: @Sendable () -> Void = { action() }
        perform(work)
    }

    public func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) {
        let timer = Timer(
            fire: date.date,
            interval: 0,
            repeats: false,
            block: { _ in action() }
        )
        timer.tolerance = tolerance.timeInterval
        add(timer, forMode: .default)
    }

    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        let timer = Timer(
            fire: date.date,
            interval: interval.timeInterval,
            repeats: true,
            block: { _ in action() }
        )
        timer.tolerance = tolerance.timeInterval
        add(timer, forMode: .default)
        return AnyCancellable { timer.invalidate() }
    }

    public var now: SchedulerTimeType {
        .init(Date())
    }

    public var minimumTolerance: SchedulerTimeType.Stride {
        .init(0)
    }
}
