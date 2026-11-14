/// A connection between a subscriber and a publisher.
///
/// Official API verified against Apple DocC:
/// `protocol Subscription : Cancellable, CustomCombineIdentifierConvertible`
/// with the single requirement `request(_:)`.
public protocol Subscription: Cancellable, CustomCombineIdentifierConvertible {
    /// Requests a specific maximum number of values.
    ///
    /// - Parameter demand: The number of values to request. Combine's demand
    ///   arithmetic saturates; a demand can never be negative.
    func request(_ demand: Subscribers.Demand)
}
