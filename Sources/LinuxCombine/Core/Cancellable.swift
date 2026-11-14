/// A cancellable task.
///
/// Official API verified against Apple DocC: `cancel()` plus two `store(in:)`
/// overloads documented on the protocol itself (`inout Set<AnyCancellable>` and
/// `inout RangeReplaceableCollection` of `AnyCancellable`).
public protocol Cancellable {
    /// Cancels the task. Cancellation is idempotent: underlying work is torn
    /// down at most once.
    func cancel()
}

extension Cancellable {
    /// Stores this cancellable in the given set for lifetime management.
    public func store(in set: inout Set<AnyCancellable>) {
        set.insert(AnyCancellable(self))
    }

    /// Stores this cancellable in the given collection for lifetime management.
    public func store<Cancellables: RangeReplaceableCollection>(
        in collection: inout Cancellables
    ) where Cancellables.Element == AnyCancellable {
        collection.append(AnyCancellable(self))
    }
}
