/// A publisher that never publishes any values, and optionally finishes
/// immediately.
///
/// Official API verified against Apple DocC: `struct Empty<Output, Failure>`,
/// `init(completeImmediately:)`, `init(completeImmediately:outputType:failureType:)`,
/// `let completeImmediately`, unconditional `Equatable`.
///
/// Timing: the subscriber receives a subscription (`Subscriptions.empty`)
/// followed immediately by `.finished` — the completion is not demand-gated.
/// With `completeImmediately: false` the publisher never completes.
public struct Empty<Output, Failure: Error>: Publisher {

    /// A Boolean value that indicates whether the publisher immediately
    /// sends a completion.
    public let completeImmediately: Bool

    public init(completeImmediately: Bool = true) {
        self.completeImmediately = completeImmediately
    }

    public init(
        completeImmediately: Bool = true,
        outputType: Output.Type,
        failureType: Failure.Type
    ) {
        self.init(completeImmediately: completeImmediately)
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        subscriber.receive(subscription: Subscriptions.empty)
        if completeImmediately {
            subscriber.receive(completion: .finished)
        }
    }
}

extension Empty: Equatable {}
