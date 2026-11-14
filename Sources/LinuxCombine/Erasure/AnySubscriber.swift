/// A type-erasing subscriber.
///
/// Official API verified against Apple DocC: `@frozen struct
/// AnySubscriber<Input, Failure>` conforming to `Subscriber`,
/// `CustomCombineIdentifierConvertible`, `CustomStringConvertible`,
/// `CustomReflectable`, `CustomPlaygroundDisplayConvertible`; inits
/// `init(_:)` for subscribers, `init(_:)` for subjects, and the closure-based
/// `init(receiveSubscription:receiveValue:receiveCompletion:)`.
@frozen public struct AnySubscriber<Input, Failure: Error>: Subscriber,
    CustomStringConvertible,
    CustomReflectable,
    CustomPlaygroundDisplayConvertible
{

    private let receiveSubscriptionThunk: (any Subscription) -> Void
    private let receiveValueThunk: (Input) -> Subscribers.Demand
    private let receiveCompletionThunk: (Subscribers.Completion<Failure>) -> Void
    private let descriptionThunk: () -> String
    private let customMirrorThunk: () -> Mirror
    private let playgroundDescriptionThunk: () -> Any

    /// A unique identifier for identifying publisher streams.
    ///
    /// Wrapping preserves the wrapped subscriber's identifier; the closure-based
    /// init creates a fresh one.
    public let combineIdentifier: CombineIdentifier

    /// Creates a type-erasing subscriber to wrap an existing subscriber.
    public init<S: Subscriber>(_ subscriber: S)
    where S.Input == Input, S.Failure == Failure {
        if let erased = subscriber as? AnySubscriber<Input, Failure> {
            self = erased
            return
        }
        combineIdentifier = subscriber.combineIdentifier
        receiveSubscriptionThunk = { subscriber.receive(subscription: $0) }
        receiveValueThunk = { subscriber.receive($0) }
        receiveCompletionThunk = { subscriber.receive(completion: $0) }
        if let customDescription = subscriber as? any CustomStringConvertible {
            let text = customDescription.description
            descriptionThunk = { text }
            playgroundDescriptionThunk = { text }
        } else {
            let name = String(describing: type(of: subscriber))
            descriptionThunk = { name }
            playgroundDescriptionThunk = { name }
        }
        let customReflectable = subscriber as? any CustomReflectable
        customMirrorThunk = {
            customReflectable?.customMirror
                ?? Mirror(subscriber, children: EmptyCollection())
        }
    }

    /// Creates a type-erasing subscriber that relays into the given subject.
    ///
    /// Upstream events are delivered into the subject via its `send` methods;
    /// the subject's subscribers' demand is relayed upstream through the
    /// internal connection.
    public init<S: Subject>(_ subject: S)
    where S.Output == Input, S.Failure == Failure {
        self.init(SubjectSubscriber(subject))
    }

    /// Creates a type-erasing subscriber that executes the provided closures.
    public init(
        receiveSubscription: ((any Subscription) -> Void)? = nil,
        receiveValue: ((Input) -> Subscribers.Demand)? = nil,
        receiveCompletion: ((Subscribers.Completion<Failure>) -> Void)? = nil
    ) {
        receiveSubscriptionThunk = receiveSubscription ?? { _ in }
        receiveValueThunk = receiveValue ?? { _ in .none }
        receiveCompletionThunk = receiveCompletion ?? { _ in }
        combineIdentifier = CombineIdentifier()
        descriptionThunk = { "Anonymous AnySubscriber" }
        customMirrorThunk = { Mirror(reflecting: "Anonymous AnySubscriber") }
        playgroundDescriptionThunk = { "Anonymous AnySubscriber" }
    }

    public func receive(subscription: any Subscription) {
        receiveSubscriptionThunk(subscription)
    }

    public func receive(_ input: Input) -> Subscribers.Demand {
        receiveValueThunk(input)
    }

    public func receive(completion: Subscribers.Completion<Failure>) {
        receiveCompletionThunk(completion)
    }

    public var description: String { descriptionThunk() }
    public var customMirror: Mirror { customMirrorThunk() }
    public var playgroundDescription: Any { playgroundDescriptionThunk() }
}
