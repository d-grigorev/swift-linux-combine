extension Subscribers {

    /// A simple subscriber that assigns received elements to a property
    /// indicated by a key path.
    ///
    /// Official API verified against Apple DocC: `final class Assign<Root,
    /// Input>`, `init(object:keyPath:)`, `object` (get), `keyPath`,
    /// `description`, `customMirror`, `playgroundDescription`, `cancel()`.
    /// Conforms to `Subscriber` (with `Failure == Never`), `Cancellable`,
    /// `CustomStringConvertible`, `CustomReflectable`,
    /// `CustomPlaygroundDisplayConvertible`.
    ///
    /// Reference semantics (documented behavior): the subscriber holds a
    /// **strong** reference to `object` until the upstream publisher calls
    /// `receive(completion:)`, at which point it is released (`object`
    /// becomes `nil`).
    public final class Assign<Root, Input>:
        Subscriber,
        Cancellable,
        CustomStringConvertible,
        CustomReflectable,
        CustomPlaygroundDisplayConvertible
    {
        public typealias Failure = Never

        private let lock = Lock()

        /// The object that contains the property to assign.
        public private(set) var object: Root?

        /// The key path that indicates the property to assign.
        public let keyPath: ReferenceWritableKeyPath<Root, Input>

        private var status = SubscriptionStatus.awaitingSubscription

        public var description: String { "Assign \(Root.self)." }

        public var customMirror: Mirror {
            let children: [Mirror.Child] = [
                ("object", object as Any),
                ("keyPath", keyPath),
                ("status", status as Any),
            ]
            return Mirror(self, children: children)
        }

        public var playgroundDescription: Any { description }

        public init(object: Root, keyPath: ReferenceWritableKeyPath<Root, Input>) {
            self.object = object
            self.keyPath = keyPath
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
            guard case .subscribed = status, let object else {
                lock.unlock()
                return .none
            }
            lock.unlock()
            object[keyPath: keyPath] = value
            return .none
        }

        public func receive(completion: Subscribers.Completion<Never>) {
            lock.lock()
            guard case .subscribed = status else {
                lock.unlock()
                return
            }
            terminateAndConsumeLock()
        }

        public func cancel() {
            lock.lock()
            guard case let .subscribed(subscription) = status else {
                lock.unlock()
                return
            }
            terminateAndConsumeLock()
            subscription.cancel()
        }

        private func terminateAndConsumeLock() {
            status = .terminal
            // Release the object after unlocking: its deinit may re-enter
            // this subscriber and deadlock on the non-recursive lock.
            withExtendedLifetime(object) {
                object = nil
                lock.unlock()
            }
        }
    }
}

extension Publisher where Failure == Never {
    /// Assigns each element from a publisher to a property on an object.
    ///
    /// The returned cancellable must be held — deallocation stops the
    /// assignment.
    public func assign<Root>(
        to keyPath: ReferenceWritableKeyPath<Root, Output>,
        on object: Root
    ) -> AnyCancellable {
        let subscriber = Subscribers.Assign(object: object, keyPath: keyPath)
        subscribe(subscriber)
        return AnyCancellable(subscriber)
    }
}
