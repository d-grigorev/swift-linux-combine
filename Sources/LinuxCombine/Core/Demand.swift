extension Subscribers {

    /// A requested number of items, sent to a publisher from a subscriber through
    /// the subscription.
    ///
    /// Official API verified against Apple DocC: `none`, `unlimited`, `max(_:)`,
    /// `max`, the arithmetic (`+`, `+=`, `-`, `-=`, `*`, `*=`) and comparison
    /// operators (Demand-Demand and Demand-Int both directions),
    /// `CustomStringConvertible`, `Codable`.
    @frozen public struct Demand: Equatable,
                                  Hashable,
                                  Comparable,
                                  Codable,
                                  CustomStringConvertible,
                                  Sendable,
                                  BitwiseCopyable
    {

        /// Count representation saturating into `unlimited`.
        /// Anything at or above `UInt(Int.max) + 1` means unlimited.
        internal let rawValue: UInt

        internal init(rawValue: UInt) {
            self.rawValue = min(UInt(Int.max) + 1, rawValue)
        }

        /// A request for as many values as the publisher can produce.
        public static let unlimited = Demand(rawValue: .max)

        /// A request for no elements from the publisher.
        /// Equivalent to `Demand.max(0)`.
        public static let none = Demand(rawValue: 0)

        /// Creates a demand for the given maximum number of elements.
        ///
        /// The publisher is free to send fewer than the requested maximum number of
        /// elements. A negative value is a programming error (fatal error).
        public static func max(_ value: Int) -> Demand {
            precondition(value >= 0, "demand cannot be negative")
            return Demand(rawValue: UInt(value))
        }

        /// The number of requested values, or `nil` when the demand is `unlimited`.
        public var max: Int? {
            self == .unlimited ? nil : Int(rawValue)
        }

        public var description: String {
            self == .unlimited ? "unlimited" : "max(\(rawValue))"
        }

        // MARK: - Addition

        /// Returns the result of adding two demands.
        /// When adding any value to `.unlimited`, the result is `.unlimited`.
        /// An arithmetic overflow produces `.unlimited`.
        public static func + (lhs: Demand, rhs: Demand) -> Demand {
            if lhs == .unlimited || rhs == .unlimited { return .unlimited }
            let (sum, overflow) = Int(lhs.rawValue).addingReportingOverflow(Int(rhs.rawValue))
            return overflow ? .unlimited : .max(sum)
        }

        /// Adds two demands, and assigns the result to the first demand.
        public static func += (lhs: inout Demand, rhs: Demand) {
            if lhs == .unlimited { return }
            lhs = lhs + rhs
        }

        /// Returns the result of adding an integer to a demand.
        /// When adding any value to `.unlimited`, the result is `.unlimited`.
        public static func + (lhs: Demand, rhs: Int) -> Demand {
            if lhs == .unlimited { return .unlimited }
            let (sum, overflow) = Int(lhs.rawValue).addingReportingOverflow(rhs)
            return overflow ? .unlimited : .max(sum)
        }

        /// Adds an integer to a demand, and assigns the result to the demand.
        public static func += (lhs: inout Demand, rhs: Int) {
            lhs = lhs + rhs
        }

        // MARK: - Multiplication

        /// Returns the result of multiplying a demand by an integer.
        /// When multiplying any value by `.unlimited`, the result is `.unlimited`.
        /// An arithmetic overflow produces `.unlimited`.
        public static func * (lhs: Demand, rhs: Int) -> Demand {
            if lhs == .unlimited { return .unlimited }
            let (product, overflow) = Int(lhs.rawValue).multipliedReportingOverflow(by: rhs)
            return overflow ? .unlimited : .max(product)
        }

        /// Multiplies a demand by an integer, and assigns the result to the demand.
        public static func *= (lhs: inout Demand, rhs: Int) {
            lhs = lhs * rhs
        }

        // MARK: - Subtraction

        /// Returns the result of subtracting one demand from another.
        ///
        /// Subtracting any value (including `.unlimited`) from `.unlimited` leaves
        /// `.unlimited`. Subtracting `.unlimited` from a finite demand gives `.none`.
        /// Results that would go negative are clamped to `.none` (official docs:
        /// "any operation that would result in a negative value is clamped to
        /// `.max(0)`").
        public static func - (lhs: Demand, rhs: Demand) -> Demand {
            if lhs == .unlimited { return .unlimited }
            if rhs == .unlimited { return .none }
            let (diff, overflow) = Int(lhs.rawValue).subtractingReportingOverflow(Int(rhs.rawValue))
            return overflow || diff < 0 ? .none : .max(diff)
        }

        /// Subtracts one demand from another, and assigns the result to the first demand.
        public static func -= (lhs: inout Demand, rhs: Demand) {
            lhs = lhs - rhs
        }

        /// Returns the result of subtracting an integer from a demand.
        /// Finite results are clamped to `.none`; `.unlimited` absorbs subtraction.
        public static func - (lhs: Demand, rhs: Int) -> Demand {
            if lhs == .unlimited { return .unlimited }
            let (diff, overflow) = Int(lhs.rawValue).subtractingReportingOverflow(rhs)
            return overflow || diff < 0 ? .none : .max(diff)
        }

        /// Subtracts an integer from a demand, and assigns the result to the demand.
        public static func -= (lhs: inout Demand, rhs: Int) {
            if lhs == .unlimited { return }
            lhs = lhs - rhs
        }

        // MARK: - Comparing a demand with an integer

        /// Whether the demand requests more elements than the integer.
        /// An `.unlimited` demand is always greater.
        public static func > (lhs: Demand, rhs: Int) -> Bool {
            lhs == .unlimited ? true : Int(lhs.rawValue) > rhs
        }

        /// Whether the demand requests at least the given number of elements.
        public static func >= (lhs: Demand, rhs: Int) -> Bool {
            lhs == .unlimited ? true : Int(lhs.rawValue) >= rhs
        }

        /// Whether the integer exceeds the demand's maximum.
        /// Nothing exceeds an `.unlimited` demand.
        public static func > (lhs: Int, rhs: Demand) -> Bool {
            rhs == .unlimited ? false : lhs > Int(rhs.rawValue)
        }

        /// Whether the integer is at least the demand's maximum.
        public static func >= (lhs: Int, rhs: Demand) -> Bool {
            rhs == .unlimited ? false : lhs >= Int(rhs.rawValue)
        }

        /// Whether the demand requests fewer elements than the integer.
        public static func < (lhs: Demand, rhs: Int) -> Bool {
            lhs == .unlimited ? false : Int(lhs.rawValue) < rhs
        }

        /// Whether the integer is below the demand's maximum.
        /// Nothing is below an `.unlimited` demand.
        public static func < (lhs: Int, rhs: Demand) -> Bool {
            rhs == .unlimited ? true : lhs < Int(rhs.rawValue)
        }

        /// Whether the demand requests at most the given number of elements.
        public static func <= (lhs: Demand, rhs: Int) -> Bool {
            lhs == .unlimited ? false : Int(lhs.rawValue) <= rhs
        }

        /// Whether the integer is at most the demand's maximum.
        public static func <= (lhs: Int, rhs: Demand) -> Bool {
            rhs == .unlimited ? true : lhs <= Int(rhs.rawValue)
        }

        /// Whether the demand requests fewer elements than the second demand.
        /// Two `.unlimited` demands are never ordered below each other.
        public static func < (lhs: Demand, rhs: Demand) -> Bool {
            if lhs == .unlimited { return false }
            if rhs == .unlimited { return true }
            return lhs.rawValue < rhs.rawValue
        }

        /// Whether the demand requests at most what the second demand requests.
        public static func <= (lhs: Demand, rhs: Demand) -> Bool {
            if lhs == .unlimited { return rhs == .unlimited }
            if rhs == .unlimited { return true }
            return lhs.rawValue <= rhs.rawValue
        }

        /// Whether the demand requests at least what the second demand requests.
        public static func >= (lhs: Demand, rhs: Demand) -> Bool {
            if rhs == .unlimited { return lhs == .unlimited }
            if lhs == .unlimited { return true }
            return lhs.rawValue >= rhs.rawValue
        }

        /// Whether the demand requests more elements than the second demand.
        public static func > (lhs: Demand, rhs: Demand) -> Bool {
            if rhs == .unlimited { return false }
            if lhs == .unlimited { return true }
            return lhs.rawValue > rhs.rawValue
        }

        /// Whether the demand requests exactly the given number of elements.
        /// An `.unlimited` demand never equals an integer.
        public static func == (lhs: Demand, rhs: Int) -> Bool {
            lhs == .unlimited ? false : Int(lhs.rawValue) == rhs
        }

        /// Whether the demand requests a number of elements other than the integer.
        public static func != (lhs: Demand, rhs: Int) -> Bool {
            !(lhs == rhs)
        }

        /// Whether the given number of elements matches the demand.
        public static func == (lhs: Int, rhs: Demand) -> Bool {
            rhs == lhs
        }

        /// Whether the given number of elements differs from the demand.
        public static func != (lhs: Int, rhs: Demand) -> Bool {
            !(lhs == rhs)
        }

        // MARK: - Codable

        public init(from decoder: any Decoder) throws {
            try self.init(rawValue: decoder.singleValueContainer().decode(UInt.self))
        }

        public func encode(to encoder: any Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(rawValue)
        }
    }
}
