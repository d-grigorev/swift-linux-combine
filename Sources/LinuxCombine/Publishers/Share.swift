extension Publishers {

    /// A publisher that shares the output of an upstream publisher with
    /// multiple subscribers.
    ///
    /// Official API: `final class Share<Upstream>` conforming to
    /// `Equatable` (identity).
    ///
    /// Semantics (verified by the parity suite): a `Multicast` over a
    /// `PassthroughSubject` with an implicit `autoconnect()`.
    public final class Share<Upstream: Publisher>: Publisher, Equatable {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        private typealias MulticastSubject = PassthroughSubject<Output, Failure>

        private let inner: Publishers.Autoconnect<
            Publishers.Multicast<Upstream, MulticastSubject>
        >

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        public init(upstream: Upstream) {
            self.inner = upstream.multicast(subject: MulticastSubject()).autoconnect()
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            inner.subscribe(subscriber)
        }

        public static func == (lhs: Share, rhs: Share) -> Bool {
            lhs === rhs
        }
    }
}

extension Publisher {
    /// Shares the output of an upstream publisher with multiple
    /// subscribers.
    public func share() -> Publishers.Share<Self> {
        .init(upstream: self)
    }
}
