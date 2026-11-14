/// A service that receives elements from a publisher.
///
/// Official API verified against Apple DocC:
/// `protocol Subscriber<Input, Failure> : CustomCombineIdentifierConvertible`
/// with requirements `receive(subscription:)`, `receive(_:)`,
/// `receive(completion:)`; the Void sugar `receive()` lives in a constrained
/// extension (official `Subscriber/receive()` decl carries no where-clause of
/// its own, i.e. it is an extension member of `extension Subscriber where
/// Input == Void`).
public protocol Subscriber<Input, Failure>: CustomCombineIdentifierConvertible {
    associatedtype Input
    associatedtype Failure: Error

    /// Receives the subscription, enabling value requests.
    func receive(subscription: any Subscription)

    /// Receives a single value; returns any additional demand to request.
    func receive(_ input: Input) -> Subscribers.Demand

    /// Receives the terminal completion event.
    func receive(completion: Subscribers.Completion<Failure>)
}

extension Subscriber where Input == Void {
    /// Requests further values from a publisher of `Void` elements.
    ///
    /// Official: "Tells the subscriber that a publisher of void elements is ready
    /// to receive further requested values."
    public func receive() -> Subscribers.Demand {
        receive(())
    }
}
