extension Publishers {

    /// A publisher that prints log messages for all publishing events,
    /// optionally prefixed with a given string.
    ///
    /// Official API: `struct Print<Upstream>` with
    /// `init(upstream:prefix:to:)`.
    ///
    /// Semantics (verified by the parity suite): logs subscription, each
    /// request (and synchronous demands returned by `receive(_:)`), values,
    /// completion, and cancellation; the prefix is normalized to
    /// `"<prefix>: "` (empty prefix stays empty).
    public struct Print<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// A string with which to prefix all log messages.
        public let prefix: String

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The stream to which log messages are written (console by default).
        public let stream: (any TextOutputStream)?

        public init(upstream: Upstream, prefix: String, to stream: (any TextOutputStream)? = nil) {
            self.upstream = upstream
            self.prefix = prefix
            self.stream = stream
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, prefix: prefix, stream: stream))
        }
    }
}

extension Publisher {
    /// Prints log messages for all publishing events.
    public func print(
        _ prefix: String = "",
        to stream: (any TextOutputStream)? = nil
    ) -> Publishers.Print<Self> {
        .init(upstream: self, prefix: prefix, to: stream)
    }
}

extension Publishers.Print {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        /// A concrete type wrapper around an abstract stream.
        private struct PrintTarget: TextOutputStream {
            var stream: any TextOutputStream

            mutating func write(_ string: String) {
                stream.write(string)
            }
        }

        private let lock = Lock()
        private var downstream: Downstream
        private let prefix: String
        private var stream: PrintTarget?
        private var status = SubscriptionStatus.awaitingSubscription

        init(downstream: Downstream, prefix: String, stream: (any TextOutputStream)?) {
            self.downstream = downstream
            self.prefix = prefix.isEmpty ? "" : "\(prefix): "
            self.stream = stream.map(PrintTarget.init)
        }

        func receive(subscription: any Subscription) {
            log("\(prefix)receive subscription: (\(subscription))")
            lock.lock()
            guard case .awaitingSubscription = status else {
                lock.unlock()
                subscription.cancel()
                return
            }
            status = .subscribed(subscription)
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            log("\(prefix)receive value: (\(input))")
            let demand = downstream.receive(input)

            if let max = demand.max {
                log("\(prefix)request max: (\(max)) (synchronous)")
            } else {
                log("\(prefix)request unlimited (synchronous)")
            }

            return demand
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished:
                log("\(prefix)receive finished")
            case .failure(let error):
                log("\(prefix)receive error: (\(error))")
            }
            lock.lock()
            status = .terminal
            lock.unlock()
            downstream.receive(completion: completion)
        }

        func request(_ demand: Subscribers.Demand) {
            if let max = demand.max {
                log("\(prefix)request max: (\(max))")
            } else {
                log("\(prefix)request unlimited")
            }
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            lock.unlock()
            subscription.request(demand)
        }

        func cancel() {
            log("\(prefix)receive cancel")
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "Print" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }

        private func log(_ text: String) {
            if var stream = stream {
                Swift.print(text, to: &stream)
            } else {
                Swift.print(text)
            }
        }
    }
}
