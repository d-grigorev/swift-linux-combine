/// A publisher that lets you dynamically add subscribers and broadcast elements.
///
/// Official API verified against Apple DocC:
/// `protocol Subject<Output, Failure> : AnyObject, Publisher` with
/// `send(_:)`, `send(completion:)`, `send(subscription:)`; the Void sugar
/// `send()` is an extension member of `extension Subject where Output == Void`.
public protocol Subject<Output, Failure>: AnyObject, Publisher {
    /// Sends a value to the subscriber.
    func send(_ value: Output)

    /// Sends a completion signal to the subscriber.
    func send(completion: Subscribers.Completion<Failure>)

    /// Sends a subscription to the subscriber.
    func send(subscription: any Subscription)
}

extension Subject where Output == Void {
    /// Sends a void value to the subscriber.
    ///
    /// Official: "Sends a void value to the subscriber."
    public func send() {
        send(())
    }
}
