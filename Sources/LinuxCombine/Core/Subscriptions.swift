extension Subscriptions {

    /// The "empty" subscription: ignores requests and cancellation.
    ///
    /// Official API: `static var empty: any Subscription { get }`.
    public static var empty: any Subscription {
        EmptySubscription.shared
    }

    /// No-op singleton backing `Subscriptions.empty`.
    private struct EmptySubscription: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    {
        let combineIdentifier = CombineIdentifier()

        func request(_ demand: Subscribers.Demand) {}
        func cancel() {}

        static let shared = EmptySubscription()

        var description: String { "Empty" }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            Mirror(self, children: EmptyCollection())
        }
    }
}
