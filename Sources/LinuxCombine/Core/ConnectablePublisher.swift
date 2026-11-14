/// A publisher that requires an explicit connection to start producing elements.
///
/// Official API verified against Apple DocC:
/// `protocol ConnectablePublisher<Output, Failure> : Publisher` with
/// requirement `connect()`; `autoconnect()` is documented on the protocol but
/// returns `Publishers.Autoconnect<Self>` — that operator type is outside the
/// current core scope and lands with the connectable batch (M4).
public protocol ConnectablePublisher<Output, Failure>: Publisher {
    /// Connects to the publisher, allowing it to produce elements, and returns
    /// an instance with which to cancel the connection.
    func connect() -> any Cancellable
}
