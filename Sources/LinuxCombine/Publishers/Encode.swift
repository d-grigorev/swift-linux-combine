extension Publishers {

    /// A publisher that encodes the output from upstream using a specified
    /// encoder.
    ///
    /// Official API: `struct Encode<Upstream, Coder>` where
    /// `Upstream.Output: Encodable`, `Output == Coder.Output`,
    /// `Failure == Error`.
    ///
    /// Semantics (verified by the parity suite): an encoding error cancels
    /// the upstream and terminates with `.failure`; upstream completions
    /// are type-erased to `Error`.
    public struct Encode<Upstream: Publisher, Coder: TopLevelEncoder>: Publisher
    where Upstream.Output: Encodable {
        public typealias Failure = Error
        public typealias Output = Coder.Output

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        private let _encode: (Upstream.Output) throws -> Output

        public init(upstream: Upstream, encoder: Coder) {
            self.upstream = upstream
            self._encode = encoder.encode
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, encode: _encode))
        }
    }

    /// A publisher that decodes the output from the upstream using a
    /// specified decoder.
    ///
    /// Official API: `struct Decode<Upstream, Output, Coder>` where
    /// `Upstream.Output == Coder.Input`, `Failure == Error`.
    public struct Decode<Upstream: Publisher, Output: Decodable, Coder: TopLevelDecoder>:
        Publisher where Upstream.Output == Coder.Input {
        public typealias Failure = Error

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        private let _decode: (Upstream.Output) throws -> Output

        public init(upstream: Upstream, decoder: Coder) {
            self.upstream = upstream
            self._decode = { try decoder.decode(Output.self, from: $0) }
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(downstream: subscriber, decode: _decode))
        }
    }
}

extension Publisher {
    /// Encodes the output from upstream using a specified encoder.
    public func encode<Coder: TopLevelEncoder>(
        encoder: Coder
    ) -> Publishers.Encode<Self, Coder> {
        .init(upstream: self, encoder: encoder)
    }

    /// Decodes the output from the upstream using a specified decoder.
    public func decode<Item: Decodable, Coder: TopLevelDecoder>(
        type: Item.Type,
        decoder: Coder
    ) -> Publishers.Decode<Self, Item, Coder> where Output == Coder.Input {
        .init(upstream: self, decoder: decoder)
    }
}

extension Publishers.Encode {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private let encode: (Upstream.Output) throws -> Output
        private var finished = false
        private var subscription: (any Subscription)?

        fileprivate init(
            downstream: Downstream,
            encode: @escaping (Upstream.Output) throws -> Output
        ) {
            self.downstream = downstream
            self.encode = encode
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            if finished || self.subscription != nil {
                lock.unlock()
                subscription.cancel()
                return
            }
            self.subscription = subscription
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if finished {
                lock.unlock()
                return .none
            }
            lock.unlock()
            do {
                return try downstream.receive(encode(input))
            } catch {
                lock.lock()
                finished = true
                let subscription = self.subscription
                self.subscription = nil
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            if finished {
                lock.unlock()
                return
            }
            finished = true
            subscription = nil
            lock.unlock()
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let error):
                downstream.receive(completion: .failure(error))
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            let subscription = self.subscription
            lock.unlock()
            subscription?.request(demand)
        }

        func cancel() {
            lock.lock()
            guard !finished, let subscription = self.subscription else {
                lock.unlock()
                return
            }
            finished = true
            self.subscription = nil
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "Encode" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream),
                ("finished", finished),
                ("upstreamSubscription", subscription as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

extension Publishers.Decode {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Error {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private let downstream: Downstream
        private let decode: (Upstream.Output) throws -> Output
        private var finished = false
        private var subscription: (any Subscription)?

        fileprivate init(
            downstream: Downstream,
            decode: @escaping (Upstream.Output) throws -> Output
        ) {
            self.downstream = downstream
            self.decode = decode
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            if finished || self.subscription != nil {
                lock.unlock()
                subscription.cancel()
                return
            }
            self.subscription = subscription
            lock.unlock()
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            if finished {
                lock.unlock()
                return .none
            }
            lock.unlock()
            do {
                return try downstream.receive(decode(input))
            } catch {
                lock.lock()
                finished = true
                let subscription = self.subscription
                self.subscription = nil
                lock.unlock()
                subscription?.cancel()
                downstream.receive(completion: .failure(error))
                return .none
            }
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            if finished {
                lock.unlock()
                return
            }
            finished = true
            subscription = nil
            lock.unlock()
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let error):
                downstream.receive(completion: .failure(error))
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            let subscription = self.subscription
            lock.unlock()
            subscription?.request(demand)
        }

        func cancel() {
            lock.lock()
            guard !finished, let subscription = self.subscription else {
                lock.unlock()
                return
            }
            finished = true
            self.subscription = nil
            lock.unlock()
            subscription.cancel()
        }

        var description: String { "Decode" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("downstream", downstream),
                ("finished", finished),
                ("upstreamSubscription", subscription as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
