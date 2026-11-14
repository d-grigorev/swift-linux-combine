/// Trivial state machine shared by closure-based subscribers and operator
/// inners.
///
/// (Same shape as Apple's internal status tracking, as mirrored by the
/// parity suite's assertions.)
internal enum SubscriptionStatus {
    case awaitingSubscription
    case subscribed(any Subscription)
    /// A terminal completion is scheduled but not yet delivered (e.g.
    /// `receive(on:)` hops the completion onto its scheduler).
    case pendingTerminal(any Subscription)
    case terminal
}

extension SubscriptionStatus {
    /// The live upstream subscription, if one exists (subscribed or
    /// pending-terminal).
    var subscription: (any Subscription)? {
        switch self {
        case .subscribed(let subscription), .pendingTerminal(let subscription):
            return subscription
        case .awaitingSubscription, .terminal:
            return nil
        }
    }
}
