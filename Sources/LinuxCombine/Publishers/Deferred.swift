/// A publisher that uses a closure to create a new publisher for each
/// subscriber.
///
/// Official API verified against Apple DocC: `struct Deferred<DeferredPublisher>`
/// with explicit `typealias Output`/`typealias Failure`,
/// `let createPublisher`, `init(createPublisher:)`.
///
/// The factory runs once per subscriber — never at init time.
public struct Deferred<DeferredPublisher: Publisher>: Publisher {

    public typealias Output = DeferredPublisher.Output
    public typealias Failure = DeferredPublisher.Failure

    /// The closure that creates the publisher the subscribers receive.
    public let createPublisher: () -> DeferredPublisher

    public init(createPublisher: @escaping () -> DeferredPublisher) {
        self.createPublisher = createPublisher
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        createPublisher().receive(subscriber: subscriber)
    }
}
