import Dispatch

// Note: on this platform there is no Apple Combine to disambiguate with, so
// the conformance is added directly to `DispatchQueue` (Apple's official
// shape: nested `SchedulerTimeType`/`SchedulerOptions` + the five protocol
// witnesses), without OpenCombine's `OCombine` namespace workaround.

extension DispatchQueue {

    /// The scheduler time type used by the dispatch queue.
    public struct SchedulerTimeType: Strideable, Codable, Hashable {

        /// The dispatch time represented by this type.
        public var dispatchTime: DispatchTime

        /// Creates a dispatch queue time type instance.
        public init(_ time: DispatchTime) {
            dispatchTime = time
        }

        public func distance(to other: SchedulerTimeType) -> Stride {
            Stride(dispatchTime.polyfillDistance(to: other.dispatchTime))
        }

        public func advanced(by stride: Stride) -> SchedulerTimeType {
            stride.magnitude == .max
                ? .init(.distantFuture)
                : .init(dispatchTime + stride.timeInterval)
        }

        public func hash(into hasher: inout Hasher) {
            hasher.combine(dispatchTime.rawValue)
        }

        public func encode(to encoder: any Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(dispatchTime.uptimeNanoseconds)
        }

        public init(from decoder: any Decoder) throws {
            let container = try decoder.singleValueContainer()
            dispatchTime = try .init(uptimeNanoseconds: container.decode(UInt64.self))
        }

        /// The distance between two dispatch queue times.
        public struct Stride: SchedulerTimeIntervalConvertible,
                              Comparable,
                              SignedNumeric,
                              ExpressibleByFloatLiteral,
                              Hashable,
                              Codable {

            public typealias FloatLiteralType = Double
            public typealias IntegerLiteralType = Int
            public typealias Magnitude = Int

            private var _nanoseconds: Int64

            /// The value of this time interval in nanoseconds.
            public var magnitude: Int {
                get { Int(_nanoseconds) }
                set { _nanoseconds = Int64(newValue) }
            }

            /// A `DispatchTimeInterval` created with this value in nanoseconds.
            public var timeInterval: DispatchTimeInterval {
                .nanoseconds(magnitude)
            }

            private init(magnitude: Int64) {
                _nanoseconds = magnitude
            }

            /// Creates an interval from a `DispatchTimeInterval`.
            public init(_ timeInterval: DispatchTimeInterval) {
                switch timeInterval {
                case .seconds(let seconds):
                    self = .seconds(seconds)
                case .milliseconds(let milliseconds):
                    self = .milliseconds(milliseconds)
                case .microseconds(let microseconds):
                    self = .microseconds(microseconds)
                case .nanoseconds(let nanoseconds):
                    self = .nanoseconds(nanoseconds)
                default:
                    if case .never = timeInterval {
                        self = .nanoseconds(.max)
                    } else {
                        self = Self(__guessFromUnknown: timeInterval)
                    }
                }
            }

            /// Widest-range fallback for unknown `DispatchTimeInterval`
            /// payloads (mirrors the consulted reference; public for the
            /// parity suite's conversion tests — see quirks doc).
            public init(__guessFromUnknown timeInterval: DispatchTimeInterval) {
                let referenceTime = DispatchTime(uptimeNanoseconds: .max / 13)
                self = SchedulerTimeType(referenceTime)
                    .distance(to: SchedulerTimeType(referenceTime + timeInterval))
            }

            public init(floatLiteral value: Double) {
                self = .seconds(value)
            }

            public init(integerLiteral value: Int) {
                self = .seconds(value)
            }

            public init?<Source: BinaryInteger>(exactly source: Source) {
                guard let value = Int(exactly: source) else { return nil }
                self = .seconds(value)
            }

            public static func < (lhs: Stride, rhs: Stride) -> Bool {
                lhs._nanoseconds < rhs._nanoseconds
            }

            public static func * (lhs: Stride, rhs: Stride) -> Stride {
                // Mirrors the consulted reference: stride multiplication is a
                // no-op (result 0). Flagged in docs/QUIRKS.md if the borrowed
                // tests say otherwise.
                Stride(magnitude: 0)
            }

            public static func + (lhs: Stride, rhs: Stride) -> Stride {
                Stride(magnitude: lhs._nanoseconds + rhs._nanoseconds)
            }

            public static func - (lhs: Stride, rhs: Stride) -> Stride {
                Stride(magnitude: lhs._nanoseconds - rhs._nanoseconds)
            }

            public static func -= (lhs: inout Stride, rhs: Stride) {
                lhs._nanoseconds -= rhs._nanoseconds
            }

            public static func *= (lhs: inout Stride, rhs: Stride) {
                lhs._nanoseconds = 0
            }

            public static func += (lhs: inout Stride, rhs: Stride) {
                lhs._nanoseconds += rhs._nanoseconds
            }

            public static func seconds(_ value: Double) -> Stride {
                let nanoseconds = value * 1_000_000_000
                if nanoseconds >= Double(Int64.max) {
                    return Stride(magnitude: .max)
                }
                if nanoseconds <= Double(Int64.min) {
                    return Stride(magnitude: .min)
                }
                return Stride(magnitude: Int64(nanoseconds))
            }

            public static func seconds(_ value: Int) -> Stride {
                Stride(magnitude: clampedIntProduct(Int64(value), 1_000_000_000))
            }

            public static func milliseconds(_ value: Int) -> Stride {
                Stride(magnitude: clampedIntProduct(Int64(value), 1_000_000))
            }

            public static func microseconds(_ value: Int) -> Stride {
                Stride(magnitude: clampedIntProduct(Int64(value), 1_000))
            }

            public static func nanoseconds(_ value: Int) -> Stride {
                Stride(magnitude: Int64(value))
            }
        }
    }

    /// Options that affect the operation of the dispatch queue scheduler.
    public struct SchedulerOptions {
        /// The dispatch queue quality of service.
        public var qos: DispatchQoS

        /// The dispatch queue work item flags.
        public var flags: DispatchWorkItemFlags

        /// The dispatch group, if any, used for performing actions.
        public var group: DispatchGroup?

        public init(
            qos: DispatchQoS = .unspecified,
            flags: DispatchWorkItemFlags = [],
            group: DispatchGroup? = nil
        ) {
            self.qos = qos
            self.flags = flags
            self.group = group
        }
    }

    public var minimumTolerance: SchedulerTimeType.Stride {
        .nanoseconds(0)
    }

    public var now: SchedulerTimeType {
        .init(.now())
    }

    public func schedule(options: SchedulerOptions?, _ action: @escaping () -> Void) {
        let options = options ?? .init()
        // Wrapped for Dispatch's @Sendable parameter; the official signature
        // keeps the plain `() -> Void` (see docs/QUIRKS.md).
        let work: @Sendable () -> Void = { action() }
        async(
            group: options.group,
            qos: options.qos,
            flags: options.flags,
            execute: work
        )
    }

    public func schedule(
        after date: SchedulerTimeType,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) {
        let options = options ?? .init()
        let work: @Sendable () -> Void = { action() }
        asyncAfter(
            deadline: date.dispatchTime,
            qos: options.qos,
            flags: options.flags,
            execute: work
        )
    }

    /// Performs the action repeatedly after the specified date.
    public func schedule(
        after date: SchedulerTimeType,
        interval: SchedulerTimeType.Stride,
        tolerance: SchedulerTimeType.Stride,
        options: SchedulerOptions?,
        _ action: @escaping () -> Void
    ) -> any Cancellable {
        let options = options ?? .init()
        let timer = DispatchSource.makeTimerSource(queue: self)
        timer.setEventHandler(qos: options.qos, flags: options.flags, handler: action)
        timer.schedule(
            deadline: date.dispatchTime,
            repeating: interval.timeInterval,
            leeway: tolerance.timeInterval
        )
        timer.resume()
        return AnyCancellable(timer.cancel)
    }
}

extension DispatchQueue: Scheduler {
    // The nested `SchedulerTimeType`/`SchedulerOptions` above serve as the
    // associated types; the witness methods (`now`, `minimumTolerance`, the
    // three `schedule` overloads) are defined on DispatchQueue directly,
    // matching Apple's official conformance (`receive(on: DispatchQueue.main)`,
    // `DispatchQueue.main.schedule { … }`).
}

/// Returns `lhs * rhs`, clamped to the range `[Int64.min, Int64.max]`.
/// (`rhs` is always a positive multiplier in this module.)
private func clampedIntProduct(_ lhs: Int64, _ rhs: Int64) -> Int64 {
    assert(rhs > 0, "multiplier must be positive")
    let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
    if overflow {
        return lhs > 0 ? .max : .min
    }
    return result
}

extension DispatchTime {
    /// Distance between two dispatch times as a `DispatchTimeInterval`,
    /// polyfilled for platforms without the newer API.
    fileprivate func polyfillDistance(to other: DispatchTime) -> DispatchTimeInterval {
        let start = rawValue
        let end = other.rawValue
        if end >= start {
            let result = end &- start
            if result > UInt64(Int.max) {
                return .never
            } else {
                return .nanoseconds(Int(result))
            }
        } else {
            let result = start &- end
            if result > UInt64(Int.max) {
                return .never
            } else {
                return .nanoseconds(-Int(result))
            }
        }
    }
}
