/// A scheduler for performing synchronous actions.
///
/// Official API verified against Apple DocC: `struct ImmediateScheduler`
/// with nested `SchedulerTimeType`/`Stride`, `SchedulerOptions = Never`,
/// `shared`, `now`, `minimumTolerance`, and the three options-bearing
/// `schedule` overloads (the other five come from the protocol defaults).
/// Dates and tolerances are ignored — actions run immediately.
public struct ImmediateScheduler: Scheduler {

    /// The time type used by the immediate scheduler.
    public struct SchedulerTimeType: Strideable {

        fileprivate init() {}

        /// Returns the distance to another immediate scheduler time; always `0`.
        public func distance(to other: SchedulerTimeType) -> Stride { 0 }

        /// Advances the time by the specified amount — meaningless for the
        /// immediate scheduler; returns a fresh time value.
        public func advanced(by _: Stride) -> SchedulerTimeType {
            SchedulerTimeType()
        }

        /// The increment by which the immediate scheduler counts time.
        public struct Stride: ExpressibleByFloatLiteral,
                              Comparable,
                              SignedNumeric,
                              Codable,
                              SchedulerTimeIntervalConvertible {

            public typealias FloatLiteralType = Double
            public typealias IntegerLiteralType = Int
            public typealias Magnitude = Int

            /// The value of this time interval in seconds.
            public var magnitude: Int

            public init(_ value: Int) {
                magnitude = value
            }

            public init(integerLiteral value: Int) {
                self.init(value)
            }

            public init(floatLiteral value: Double) {
                self.init(Int(value))
            }

            public init?<Source: BinaryInteger>(exactly source: Source) {
                guard let magnitude = Int(exactly: source) else { return nil }
                self.init(magnitude)
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

            public static func -= (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude -= rhs.magnitude
            }

            public static func *= (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude *= rhs.magnitude
            }

            public static func += (lhs: inout Stride, rhs: Stride) {
                lhs.magnitude += rhs.magnitude
            }

            // The immediate scheduler has no notion of elapsed time.
            public static func seconds(_: Int) -> Stride { 0 }
            public static func seconds(_: Double) -> Stride { 0 }
            public static func milliseconds(_: Int) -> Stride { 0 }
            public static func microseconds(_: Int) -> Stride { 0 }
            public static func nanoseconds(_: Int) -> Stride { 0 }
        }
    }

    /// A type that defines options accepted by the immediate scheduler.
    public typealias SchedulerOptions = Never

    /// The shared instance of the immediate scheduler.
    ///
    /// You cannot create instances of the immediate scheduler yourself.
    public static let shared = ImmediateScheduler()

    public func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        action()
    }

    public var now: SchedulerTimeType { SchedulerTimeType() }

    public var minimumTolerance: SchedulerTimeType.Stride { 0 }

    public func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) {
        action()
    }

    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        action()
        return Subscriptions.empty
    }
}
