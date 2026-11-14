/// A publisher that immediately terminates with the specified error.
///
/// Official API verified against Apple DocC: `struct Fail<Output, Failure>`,
/// `init(error:)`, `init(outputType:failure:)`, `let error`, conditional
/// `Equatable` (where `Failure: Equatable`).
///
/// Timing: the subscriber receives a subscription (`Subscriptions.empty`)
/// followed immediately by the failure — not demand-gated.
public struct Fail<Output, Failure: Error>: Publisher {

    /// The failure to send when terminating the publisher.
    public let error: Failure

    public init(error: Failure) {
        self.error = error
    }

    public init(outputType: Output.Type, failure: Failure) {
        self.error = failure
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        subscriber.receive(subscription: Subscriptions.empty)
        subscriber.receive(completion: .failure(error))
    }
}

extension Fail: Equatable where Failure: Equatable {}
