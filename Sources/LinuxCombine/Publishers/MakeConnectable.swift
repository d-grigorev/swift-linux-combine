extension Publisher where Failure == Never {

    /// Creates a connectable wrapper around the publisher.
    ///
    /// Official API (verified against Apple DocC):
    /// `makeConnectable() -> Publishers.MakeConnectable<Self>` on
    /// `Publisher where Failure == Never`.
    public func makeConnectable() -> Publishers.MakeConnectable<Self> {
        .init(upstream: self)
    }
}

extension Publishers {

    /// A publisher that provides explicit connectability to another
    /// publisher.
    ///
    /// Official API: `struct MakeConnectable<Upstream>` conforming to
    /// `ConnectablePublisher`.
    ///
    /// Semantics (verified by the parity suite): a `Multicast` over a
    /// `PassthroughSubject` — subscribers attach immediately, but
    /// elements only flow after `connect()`.
    public struct MakeConnectable<Upstream: Publisher>: ConnectablePublisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        private let inner: Publishers.Multicast<Upstream,
                                                PassthroughSubject<Output, Failure>>

        public init(upstream: Upstream) {
            inner = upstream.multicast(subject: PassthroughSubject())
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            inner.subscribe(subscriber)
        }

        public func connect() -> any Cancellable {
            inner.connect()
        }
    }
}
