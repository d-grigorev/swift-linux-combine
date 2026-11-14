/// A protocol that provides a scheduler with an expression for relative time.
///
/// Official API verified against Apple DocC: `protocol
/// SchedulerTimeIntervalConvertible` with `seconds(Int)`, `seconds(Double)`,
/// `milliseconds(_:)`, `microseconds(_:)`, `nanoseconds(_:)`.
public protocol SchedulerTimeIntervalConvertible {
    /// Converts the specified number of seconds into an instance of this
    /// scheduler time type.
    static func seconds(_ s: Int) -> Self

    /// Converts the specified number of seconds, as a floating-point value,
    /// into an instance of this scheduler time type.
    static func seconds(_ s: Double) -> Self

    /// Converts the specified number of milliseconds into an instance of
    /// this scheduler time type.
    static func milliseconds(_ ms: Int) -> Self

    /// Converts the specified number of microseconds into an instance of
    /// this scheduler time type.
    static func microseconds(_ us: Int) -> Self

    /// Converts the specified number of nanoseconds into an instance of
    /// this scheduler time type.
    static func nanoseconds(_ ns: Int) -> Self
}

/// A protocol that provides a scheduler with an expression for relative time.
///
/// Official API verified against Apple DocC: `protocol
/// Scheduler<SchedulerTimeType>` with `SchedulerTimeType` (Strideable,
/// Stride: SchedulerTimeIntervalConvertible), `SchedulerOptions`, `now`,
/// `minimumTolerance`, and the three options-bearing `schedule` overloads as
/// requirements; the five convenience overloads below are the official
/// default implementations.
public protocol Scheduler<SchedulerTimeType> {
    associatedtype SchedulerTimeType: Strideable
        where SchedulerTimeType.Stride: SchedulerTimeIntervalConvertible
    associatedtype SchedulerOptions

    /// The current moment in time for the scheduler.
    var now: SchedulerTimeType { get }

    /// The minimum tolerance allowed by the scheduler.
    var minimumTolerance: SchedulerTimeType.Stride { get }

    /// Performs the action at some time in the future, taking options.
    func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void)

    /// Performs the action after a date, with tolerance and options.
    func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    )

    /// Performs the action repeatedly after a date, with tolerance/options.
    func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) -> any Cancellable
}

extension Scheduler {
    /// Performs the action at the next possible opportunity, without options.
    public func schedule(_ action: @escaping () -> Void) {
        schedule(options: nil, action)
    }

    /// Performs the action at some time after the specified date, using the
    /// scheduler's minimum tolerance.
    public func schedule(after date: SchedulerTimeType, _ action: @escaping () -> Void) {
        schedule(after: date, tolerance: minimumTolerance, action)
    }

    /// Performs the action at some time after the specified date.
    public func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        _ action: @escaping () -> Void
    ) {
        schedule(after: date, tolerance: tolerance, options: nil, action)
    }

    /// Performs the action at the specified frequency, using minimum tolerance.
    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        schedule(after: date, interval: interval, tolerance: minimumTolerance, action)
    }

    /// Performs the action at the specified frequency, taking tolerance
    /// into account if possible.
    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        schedule(after: date, interval: interval, tolerance: tolerance, options: nil, action)
    }
}
