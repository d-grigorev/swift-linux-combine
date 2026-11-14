extension ConnectablePublisher {

    /// Automates the process of connecting or disconnecting from this
    /// connectable publisher.
    ///
    /// Official API (verified against Apple DocC on the
    /// `ConnectablePublisher` protocol):
    /// `autoconnect() -> Publishers.Autoconnect<Self>`.
    public func autoconnect() -> Publishers.Autoconnect<Self> {
        .init(upstream: self)
    }
}

extension Publishers {

    /// A publisher that automatically connects to an upstream
    /// connectable publisher.
    ///
    /// Official API: `class Autoconnect<Upstream>` where `Upstream:
    /// ConnectablePublisher`.
    ///
    /// Semantics (verified by the parity suite): the first subscriber
    /// triggers `connect()` (after the subject-side subscription is
    /// attached); subscribers share one connection counted by
    /// reference; when the last subscriber cancels, the connection is
    /// cancelled and the state returns to `disconnected` (so a later
    /// subscriber connects anew).
    public class Autoconnect<Upstream: ConnectablePublisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        fileprivate enum State {
            case disconnected
            case connected(refcount: Int, connection: any Cancellable)
        }

        /// The publisher from which this publisher receives elements.
        public final let upstream: Upstream

        fileprivate let lock = Lock()
        fileprivate var state = State.disconnected

        public init(upstream: Upstream) {
            self.upstream = upstream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            let inner = Inner(parent: self, downstream: subscriber)
            lock.lock()
            switch state {
            case let .connected(refcount, connection):
                state = .connected(refcount: refcount + 1, connection: connection)
                lock.unlock()
                upstream.subscribe(inner)
            case .disconnected:
                lock.unlock()
                upstream.subscribe(inner)
                let connection = upstream.connect()
                lock.lock()
                state = .connected(refcount: 1, connection: connection)
                lock.unlock()
            }
        }

        fileprivate func willCancel() {
            lock.lock()
            switch state {
            case let .connected(refcount, connection):
                if refcount <= 1 {
                    state = .disconnected
                    lock.unlock()
                    connection.cancel()
                } else {
                    state = .connected(refcount: refcount - 1, connection: connection)
                    lock.unlock()
                }
            case .disconnected:
                lock.unlock()
            }
        }
    }
}

extension Publishers.Autoconnect {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        let combineIdentifier = CombineIdentifier()

        private let parent: Publishers.Autoconnect<Upstream>
        private let downstream: Downstream

        fileprivate init(parent: Publishers.Autoconnect<Upstream>,
                         downstream: Downstream) {
            self.parent = parent
            self.downstream = downstream
        }

        fileprivate func receive(subscription: any Subscription) {
            let sideEffectSubscription = SideEffectSubscription(subscription,
                                                               parent: parent)
            downstream.receive(subscription: sideEffectSubscription)
        }

        fileprivate func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            downstream.receive(input)
        }

        fileprivate func receive(completion: Subscribers.Completion<Failure>) {
            downstream.receive(completion: completion)
        }

        fileprivate var description: String { "Autoconnect" }

        fileprivate var customMirror: Mirror {
            let children: [Mirror.Child] = [
                ("parent", parent),
                ("downstream", downstream),
            ]
            return Mirror(self, children: children)
        }

        fileprivate var playgroundDescription: Any { description }
    }

    /// Wraps the upstream subscription so that a downstream `cancel()`
    /// decrements the shared connection's reference count.
    private struct SideEffectSubscription: Subscription,
        CustomStringConvertible, CustomPlaygroundDisplayConvertible
    {
        private let parent: Publishers.Autoconnect<Upstream>
        private let upstreamSubscription: any Subscription

        fileprivate init(_ upstreamSubscription: any Subscription,
                         parent: Publishers.Autoconnect<Upstream>) {
            self.parent = parent
            self.upstreamSubscription = upstreamSubscription
        }

        fileprivate func request(_ demand: Subscribers.Demand) {
            upstreamSubscription.request(demand)
        }

        fileprivate func cancel() {
            parent.willCancel()
            upstreamSubscription.cancel()
        }

        fileprivate var combineIdentifier: CombineIdentifier {
            upstreamSubscription.combineIdentifier
        }

        fileprivate var description: String {
            String(describing: upstreamSubscription)
        }

        var playgroundDescription: Any { description }
    }
}
