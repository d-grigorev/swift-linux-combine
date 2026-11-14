/// A publisher that broadcasts elements to one or more subscribers.
///
/// Official API verified against Apple DocC:
/// `protocol Publisher<Output, Failure>` with the single requirement
/// `receive(subscriber:)`.
public protocol Publisher<Output, Failure> {
    associatedtype Output
    associatedtype Failure: Error

    /// Attaches the specified subscriber to this publisher.
    ///
    /// Adopters must implement this method; callers should use
    /// `subscribe(_:)`, which forwards here.
    func receive<S>(subscriber: S)
    where S: Subscriber, Self.Failure == S.Failure, Self.Output == S.Input
}

extension Publisher {
    /// Attaches the specified subscriber to this publisher.
    ///
    /// Official: "Always call this function instead of `receive(subscriber:)`."
    public func subscribe<S: Subscriber>(_ subscriber: S)
    where S.Failure == Failure, S.Input == Output {
        receive(subscriber: subscriber)
    }

    /// Attaches the specified subject to this publisher.
    ///
    /// Elements the publisher produces are forwarded into the subject via
    /// `send(_:)`; the returned cancellable controls (and cancels) the upstream
    /// subscription. Subject subscribers' demand is relayed upstream through
    /// the returned connection.
    public func subscribe<S: Subject>(_ subject: S) -> AnyCancellable
    where S.Output == Output, S.Failure == Failure {
        let relay = SubjectSubscriber(subject)
        subscribe(relay)
        return AnyCancellable(relay)
    }

    /// Wraps this publisher with a type eraser.
    public func eraseToAnyPublisher() -> AnyPublisher<Output, Failure> {
        AnyPublisher(self)
    }
}
