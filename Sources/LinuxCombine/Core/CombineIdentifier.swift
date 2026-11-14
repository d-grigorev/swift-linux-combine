import Foundation

/// A unique identifier for identifying publisher streams.
///
/// Official API verified against Apple DocC: `init()`, `init(_:)`
/// ("using the bit pattern of the provided object"), `description`;
/// `Equatable` + `Hashable`.
///
/// Representation (verified against the Apple-validated parity suite):
/// a single `UInt64` stored property `rawValue` — visible to `Mirror` —
/// formatted as `0x…` in `description`.
public struct CombineIdentifier: Hashable, CustomStringConvertible {

    private static let lock = Lock()
    nonisolated(unsafe) private static var counter: UInt64 = 0

    private let rawValue: UInt64

    /// Creates a unique Combine identifier.
    public init() {
        Self.lock.lock()
        Self.counter += 1
        let value = Self.counter
        Self.lock.unlock()
        rawValue = value
    }

    /// Creates a Combine identifier, using the bit pattern of the provided object.
    public init(_ obj: AnyObject) {
        rawValue = UInt64(UInt(bitPattern: ObjectIdentifier(obj)))
    }

    public var description: String {
        "0x\(String(rawValue, radix: 16))"
    }
}
