extension Publishers {

    /// A strategy for filling a buffer.
    ///
    /// Official API: cases `keepFull` and `byRequest`
    /// (`Equatable` + `Hashable` conformance).
    public enum PrefetchStrategy {
        /// Fill the buffer at subscription time (`.max(size)`) and keep
        /// it full thereafter.
        case keepFull
        /// Avoid prefetching; the buffer starts by pulling `.unlimited`
        /// from the upstream (as pinned by the parity suite).
        case byRequest
    }

    /// A strategy that handles exhaustion of a buffer's capacity.
    ///
    /// Official API: cases `dropNewest`, `dropOldest`, `customError(_:)`.
    public enum BufferingStrategy<Failure: Error> {
        /// When the buffer is full, discard the newly received element.
        case dropNewest
        /// When the buffer is full, discard the oldest element in the
        /// buffer.
        case dropOldest
        /// When the buffer is full, execute the closure to provide a
        /// custom error.
        case customError(() -> Failure)
    }

    /// A publisher that buffers elements received from an upstream
    /// publisher.
    ///
    /// Official API: `struct Buffer<Upstream>` with
    /// `init(upstream:size:prefetch:whenFull:)`.
    ///
    /// Semantics (verified by the parity suite): upstream demand is
    /// established at subscription (before the downstream is granted);
    /// a single drain loop delivers buffered values against
    /// accumulated downstream demand (reentrancy-safe via a recursion
    /// flag); `.finished` is delivered once the buffer is empty under
    /// demand; a `.failure` terminal is delivered even without demand;
    /// delivered elements replenish upstream demand under `keepFull`.
    public struct Buffer<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The maximum number of elements to store.
        public let size: Int

        /// The strategy for initially populating the buffer.
        public let prefetch: Publishers.PrefetchStrategy

        /// The action to take when the buffer becomes full.
        public let whenFull: Publishers.BufferingStrategy<Failure>

        public init(
            upstream: Upstream,
            size: Int,
            prefetch: Publishers.PrefetchStrategy,
            whenFull: Publishers.BufferingStrategy<Failure>
        ) {
            self.upstream = upstream
            self.size = size
            self.prefetch = prefetch
            self.whenFull = whenFull
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                size: size,
                prefetch: prefetch,
                whenFull: whenFull
            ))
        }
    }
}

extension Publisher {
    /// Buffers elements received from an upstream publisher.
    public func buffer(
        size: Int,
        prefetch: Publishers.PrefetchStrategy,
        whenFull: Publishers.BufferingStrategy<Failure>
    ) -> Publishers.Buffer<Self> {
        .init(upstream: self, size: size, prefetch: prefetch, whenFull: whenFull)
    }
}

extension Publishers.PrefetchStrategy: Equatable {}
extension Publishers.PrefetchStrategy: Hashable {}

extension Publishers.Buffer {

    private final class Inner<Downstream: Subscriber>: Subscriber, Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let lock = Lock()
        private var recursion = false
        private let size: Int
        private let prefetch: Publishers.PrefetchStrategy
        private let whenFull: Publishers.BufferingStrategy<Failure>
        private let downstream: Downstream
        private var state = SubscriptionStatus.awaitingSubscription
        private var downstreamDemand = Subscribers.Demand.none
        private var values = [Input]()
        private var terminal: Subscribers.Completion<Failure>?

        init(
            downstream: Downstream,
            size: Int,
            prefetch: Publishers.PrefetchStrategy,
            whenFull: Publishers.BufferingStrategy<Failure>
        ) {
            self.size = size
            self.prefetch = prefetch
            self.whenFull = whenFull
            self.downstream = downstream
        }

        func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = state else {
                lock.unlock()
                subscription.cancel()
                return
            }
            state = .subscribed(subscription)
            lock.unlock()

            // Upstream demand first, THEN grant the downstream (official
            // order — pinned by the parity request-history tests).
            let upstreamDemand: Subscribers.Demand
            switch prefetch {
            case .keepFull:
                upstreamDemand = .max(size)
            case .byRequest:
                upstreamDemand = .unlimited
            }
            subscription.request(upstreamDemand)
            downstream.receive(subscription: self)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return .none
            }
            switch terminal {
            case nil, .finished?:
                if values.count >= size {
                    switch whenFull {
                    case .dropNewest:
                        lock.unlock()
                        return drain()
                    case .dropOldest:
                        values.removeFirst()
                    case let .customError(makeError):
                        terminal = .failure(makeError())
                        lock.unlock()
                        subscription.cancel()
                        return .none
                    }
                }

                values.append(input)
                lock.unlock()
                return drain()
            case .failure?:
                lock.unlock()
                return .none
            }
        }

        func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            lock.lock()
            guard case .subscribed = state, terminal == nil else {
                lock.unlock()
                return
            }
            terminal = completion
            lock.unlock()
            _ = drain()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            downstreamDemand += demand
            let recursion = self.recursion
            lock.unlock()
            if recursion {
                // The outer drain loop will pick the demand up.
                return
            }

            let more = drain()
            if more != .none {
                subscription.request(more)
            }
        }

        func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = state else {
                lock.unlock()
                return
            }
            state = .terminal
            values = []
            lock.unlock()
            subscription.cancel()
        }

        /// Delivers as much of the buffer as downstream demand allows.
        /// Returns the upstream demand accumulated while draining
        /// (nonzero only under `keepFull`).
        private func drain() -> Subscribers.Demand {
            var upstreamDemand = Subscribers.Demand.none
            lock.lock()
            while true {
                guard case .subscribed = state else {
                    lock.unlock()
                    return upstreamDemand
                }

                if downstreamDemand > 0 {
                    if values.isEmpty {
                        if let completion = terminal {
                            state = .terminal
                            lock.unlock()
                            downstream.receive(completion: completion)
                        } else {
                            lock.unlock()
                        }
                        return upstreamDemand
                    }
                } else {
                    if let completion = terminal, case .failure = completion {
                        state = .terminal
                        lock.unlock()
                        downstream.receive(completion: completion)
                    } else {
                        lock.unlock()
                    }
                    return upstreamDemand
                }

                let poppedValues = lockedPop(downstreamDemand)
                assert(poppedValues.count > 0,
                       "We checked the buffer is not empty and demand is nonzero")
                downstreamDemand -= poppedValues.count

                recursion = true
                lock.unlock()

                var newDownstreamDemand = Subscribers.Demand.none
                var additionalUpstreamDemand = 0

                for value in poppedValues {
                    newDownstreamDemand += downstream.receive(value)
                    additionalUpstreamDemand += 1
                }

                if prefetch == .keepFull {
                    upstreamDemand += additionalUpstreamDemand
                }

                lock.lock()
                recursion = false
                downstreamDemand += newDownstreamDemand
            }
        }

        private func lockedPop(_ demand: Subscribers.Demand) -> [Input] {
            assert(demand > 0)
            guard let max = demand.max else {
                let all = values
                values = []
                return all
            }
            let poppedValues = Array(values.prefix(max))
            values.removeFirst(poppedValues.count)
            return poppedValues
        }

        var description: String { "Buffer" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("values", values),
                ("state", state),
                ("downstreamDemand", downstreamDemand),
                ("terminal", terminal as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
