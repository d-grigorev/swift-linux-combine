import Foundation

/// Minimal lock wrapper used throughout the module.
///
/// Combine's types are riddled with re-entrant and cross-thread delivery;
/// every piece of mutable subscription/subject state sits behind this.
internal final class Lock: @unchecked Sendable {
    private let nsLock = NSLock()

    @inline(__always)
    internal func lock() {
        nsLock.lock()
    }

    @inline(__always)
    internal func unlock() {
        nsLock.unlock()
    }

    @inline(__always)
    internal func withLock<T>(_ body: () throws -> T) rethrows -> T {
        nsLock.lock()
        defer { nsLock.unlock() }
        return try body()
    }
}

/// Recursive lock used to serialize (and allow re-entrant) downstream callbacks.
///
/// Combine delivers elements to a subscriber with a recursive lock: a
/// `send(_:)` issued from *inside* a `receive(_:)` callback re-enters the
/// same conduit's delivery path without deadlocking while still serializing
/// cross-thread deliveries per subscription.
internal final class RecursiveLock: @unchecked Sendable {
    private let nsRecursiveLock = NSRecursiveLock()

    @inline(__always)
    internal func lock() {
        nsRecursiveLock.lock()
    }

    @inline(__always)
    internal func unlock() {
        nsRecursiveLock.unlock()
    }
}
