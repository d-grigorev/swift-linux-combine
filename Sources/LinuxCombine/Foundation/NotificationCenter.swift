import Foundation

extension NotificationCenter {

    /// A publisher that emits elements when broadcasting notifications.
    ///
    /// Official API verified against Apple DocC: nested
    /// `NotificationCenter.Publisher` with `init(center:name:object:)` and
    /// `center`/`name`/`object` properties; `publisher(for:object:)` factory.
    public struct Publisher: LinuxCombine.Publisher {
        public typealias Output = Notification
        public typealias Failure = Never

        /// The notification center this publisher uses as a source.
        public let center: NotificationCenter

        /// The name of notifications published by this publisher.
        public let name: Notification.Name

        /// The object posting the named notification.
        public let object: AnyObject?

        /// Creates a publisher that emits events when broadcasting notifications.
        public init(
            center: NotificationCenter,
            name: Notification.Name,
            object: AnyObject? = nil
        ) {
            self.center = center
            self.name = name
            self.object = object
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Notification, S.Failure == Never {
            let subscription = Notification.NotificationSubscription(
                center: center,
                name: name,
                object: object,
                downstream: subscriber
            )
            subscriber.receive(subscription: subscription)
        }
    }

    /// Returns a publisher that emits events when broadcasting notifications.
    public func publisher(
        for name: Notification.Name,
        object: AnyObject? = nil
    ) -> Publisher {
        .init(center: self, name: name, object: object)
    }
}

extension NotificationCenter.Publisher: Equatable {
    public static func == (lhs: NotificationCenter.Publisher, rhs: NotificationCenter.Publisher) -> Bool {
        lhs.center === rhs.center && lhs.name == rhs.name && lhs.object === rhs.object
    }
}

extension Notification {

    /// The subscription handed out by `NotificationCenter.Publisher`:
    /// registers the observer immediately and gates delivery on demand
    /// (notifications arriving with zero demand are dropped).
    fileprivate final class NotificationSubscription<S: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where S.Input == Notification, S.Failure == Never {
        private let lock = Lock()
        fileprivate let downstreamLock = RecursiveLock()
        fileprivate var demand = Subscribers.Demand.none
        private var center: NotificationCenter?
        private let name: Name
        private var object: AnyObject?
        private var observation: AnyObject?

        fileprivate init(
            center: NotificationCenter,
            name: Notification.Name,
            object: AnyObject?,
            downstream: S
        ) {
            self.center = center
            self.name = name
            self.object = object
            self.observation = center.addObserver(
                forName: name,
                object: object,
                queue: nil
            ) { [weak self] in
                self?.didReceiveNotification($0, downstream: downstream)
            }
        }

        private func didReceiveNotification(_ notification: Notification, downstream: S) {
            lock.lock()
            guard demand > 0 else {
                lock.unlock()
                return
            }
            demand -= 1
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(notification)
            downstreamLock.unlock()
            lock.lock()
            demand += newDemand
            lock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            self.demand += demand
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            guard let center = center, let observation = observation else {
                lock.unlock()
                return
            }
            self.center = nil
            self.observation = nil
            object = nil
            lock.unlock()
            center.removeObserver(observation)
        }

        var description: String { "NotificationCenter Observer" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("center", center as Any),
                ("name", name),
                ("object", object as Any),
                ("demand", demand),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
