/// A type-erasing cancellable object that executes a provided closure when canceled.
///
/// An `AnyCancellable` instance automatically calls `cancel()` when
/// deinitialized, and cancels at most once.
///
/// Official API verified against Apple DocC: `final class AnyCancellable`
/// conforming to `Cancellable`, `Equatable`, `Hashable`; inits `init(_:)`
/// (closure and canceller forms), the `store(in:)` pair, and `==` — equality
/// compares "whether their references point to the same instance".
///
/// Performance note (docs/PERF.md): cancel/deinit are intentionally
/// **unsynchronized**, matching Apple's implementation — a per-instance lock
/// measured 4x slower on store/release churn.
public final class AnyCancellable: Cancellable, Hashable {

    private var cancelThunk: (() -> Void)?

    /// Initializes the cancellable object with the given cancel-time closure.
    public init(_ cancel: @escaping () -> Void) {
        cancelThunk = cancel
    }

    /// Initializes the cancellable object wrapping another cancellable.
    public init<C: Cancellable>(_ canceller: C) {
        cancelThunk = { canceller.cancel() }
    }

    /// Cancels the underlying task. Idempotent: runs the cancel closure at most once.
    public func cancel() {
        cancelThunk?()
        cancelThunk = nil
    }

    deinit {
        cancelThunk?()
    }

    public static func == (lhs: AnyCancellable, rhs: AnyCancellable) -> Bool {
        ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }

    /// Stores this type-erasing cancellable instance in the specified set.
    public func store(in set: inout Set<AnyCancellable>) {
        set.insert(self)
    }

    /// Stores this type-erasing cancellable instance in the specified collection.
    public func store<Cancellables: RangeReplaceableCollection>(
        in collection: inout Cancellables
    ) where Cancellables.Element == AnyCancellable {
        collection.append(self)
    }
}
