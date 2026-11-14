extension Subscribers {

    /// A simple subscriber that requests an unlimited number of values upon
    /// subscription.
    ///
    /// Official API verified against Apple DocC: `final class Sink<Input,
    /// Failure>` with `init(receiveCompletion:receiveValue:)`, the stored
    /// `receiveValue`/`receiveCompletion` closures; conforms to `Subscriber`,
    /// `Cancellable`, `CustomStringConvertible`, `CustomReflectable`,
    /// `CustomPlaygroundDisplayConvertible`.
    public final class Sink<Input, Failure: Error>:
        Subscriber,
        Cancellable,
        CustomStringConvertible,
        CustomReflectable,
        CustomPlaygroundDisplayConvertible
    {

        /// The closure to execute on receipt of a value.
        public var receiveValue: (Input) -> Void

        /// The closure to execute on completion.
        public var receiveCompletion: (Subscribers.Completion<Failure>) -> Void

        private var status = SubscriptionStatus.awaitingSubscription
        private let lock = Lock()

        public var description: String { "Sink" }

        public var customMirror: Mirror {
            Mirror(self, children: EmptyCollection())
        }

        public var playgroundDescription: Any { description }

        public init(
            receiveCompletion: @escaping (Subscribers.Completion<Failure>) -> Void,
            receiveValue: @escaping (Input) -> Void
        ) {
            self.receiveCompletion = receiveCompletion
            self.receiveValue = receiveValue
        }

        public func receive(subscription: any Subscription) {
            lock.lock()
            guard case .awaitingSubscription = status else {
                lock.unlock()
                subscription.cancel()
                return
            }
            status = .subscribed(subscription)
            lock.unlock()
            subscription.request(.unlimited)
        }

        public func receive(_ value: Input) -> Subscribers.Demand {
            lock.lock()
            let receiveValue = self.receiveValue
            lock.unlock()
            receiveValue(value)
            return .none
        }

        public func receive(completion: Subscribers.Completion<Failure>) {
            lock.lock()
            status = .terminal
            let receiveCompletion = self.receiveCompletion
            self.receiveCompletion = { _ in }

            // The closures must be released AFTER unlocking: releasing can
            // run arbitrary user code (deinit of captured objects), which may
            // re-enter this subscriber and deadlock on the non-recursive lock.
            withExtendedLifetime(receiveValue) {
                receiveValue = { _ in }
                lock.unlock()
            }

            receiveCompletion(completion)
        }

        public func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            status = .terminal

            // See receive(completion:) for why closures are released unlocked.
            withExtendedLifetime((receiveValue, receiveCompletion)) {
                receiveCompletion = { _ in }
                receiveValue = { _ in }
                lock.unlock()
            }
            subscription.cancel()
        }
    }
}

extension Publisher {
    /// Attaches a subscriber with closure-based behavior.
    ///
    /// Creates the subscriber and immediately requests an unlimited number of
    /// values, prior to returning. The returned cancellable must be held —
    /// deallocation tears the stream down.
    public func sink(
        receiveCompletion: @escaping (Subscribers.Completion<Failure>) -> Void,
        receiveValue: @escaping (Output) -> Void
    ) -> AnyCancellable {
        let subscriber = Subscribers.Sink<Output, Failure>(
            receiveCompletion: receiveCompletion,
            receiveValue: receiveValue
        )
        subscribe(subscriber)
        return AnyCancellable(subscriber)
    }
}

extension Publisher where Failure == Never {
    /// Attaches a subscriber with closure-based behavior to a publisher that
    /// never fails.
    public func sink(receiveValue: @escaping (Output) -> Void) -> AnyCancellable {
        let subscriber = Subscribers.Sink<Output, Failure>(
            receiveCompletion: { _ in },
            receiveValue: receiveValue
        )
        subscribe(subscriber)
        return AnyCancellable(subscriber)
    }
}
