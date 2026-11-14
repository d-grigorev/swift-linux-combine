import Foundation

/// A type of object with a publisher that emits before the object has changed.
///
/// Official API verified against Apple DocC: `protocol ObservableObject:
/// AnyObject` with `ObjectWillChangePublisher` (defaulted to
/// `ObservableObjectPublisher`, `Failure == Never`) and `objectWillChange`.
///
/// Semantics (consulted reference): the default `objectWillChange` walks
/// the instance's `Mirror`, installing **one shared** lazily-created
/// `ObservableObjectPublisher` into every `@Published` property.
public protocol ObservableObject: AnyObject {

    /// The type of publisher that emits before the object has changed.
    associatedtype ObjectWillChangePublisher: Publisher = ObservableObjectPublisher
        where ObjectWillChangePublisher.Failure == Never

    /// A publisher that emits before the object has changed.
    var objectWillChange: ObjectWillChangePublisher { get }
}

private protocol _ObservableObjectProperty {
    var objectWillChange: ObservableObjectPublisher? { get nonmutating set }
}

extension Published: _ObservableObjectProperty {}

extension ObservableObject where ObjectWillChangePublisher == ObservableObjectPublisher {

    /// A publisher that emits before the object has changed.
    public var objectWillChange: ObservableObjectPublisher {
        var installedPublisher: ObservableObjectPublisher?
        var reflection: Mirror? = Mirror(reflecting: self)
        while let aClass = reflection {
            for (_, property) in aClass.children {
                guard let property = property as? _ObservableObjectProperty else {
                    // Visit other fields until we meet a @Published field.
                    continue
                }
                if let alreadyInstalledPublisher = property.objectWillChange {
                    installedPublisher = alreadyInstalledPublisher
                    // All @Published fields already share a publisher.
                    break
                }
                var lazilyCreatedPublisher: ObjectWillChangePublisher {
                    if let publisher = installedPublisher {
                        return publisher
                    }
                    let publisher = ObservableObjectPublisher()
                    installedPublisher = publisher
                    return publisher
                }
                property.objectWillChange = lazilyCreatedPublisher
            }
            reflection = aClass.superclassMirror
        }
        return installedPublisher ?? ObservableObjectPublisher()
    }
}

/// A publisher that publishes changes from observable objects.
public final class ObservableObjectPublisher: Publisher {
    public typealias Output = Void
    public typealias Failure = Never

    private let lock = Lock()
    private var connections = Set<Conduit>()

    /// Creates an observable object publisher instance.
    public init() {}

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Void, S.Failure == Never {
        let inner = Inner(downstream: subscriber, parent: self)
        lock.lock()
        connections.insert(inner)
        lock.unlock()
        subscriber.receive(subscription: inner)
    }

    /// Publishes a change (emits `()` to every requesting subscriber).
    public func send() {
        lock.lock()
        let connections = self.connections
        lock.unlock()
        for connection in connections {
            connection.send()
        }
    }

    fileprivate func remove(_ conduit: Conduit) {
        lock.lock()
        connections.remove(conduit)
        lock.unlock()
    }
}

extension ObservableObjectPublisher {

    fileprivate class Conduit: Hashable {
        fileprivate func send() {
            fatalError("abstract method")
        }

        fileprivate static func == (lhs: Conduit, rhs: Conduit) -> Bool {
            lhs === rhs
        }

        fileprivate func hash(into hasher: inout Hasher) {
            hasher.combine(ObjectIdentifier(self))
        }
    }

    private final class Inner<Downstream: Subscriber>
        : Conduit, Subscription,
          CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Void, Downstream.Failure == Never {
        private enum State {
            case initialized
            case active
            case terminal
        }

        private weak var parent: ObservableObjectPublisher?
        private let downstream: Downstream
        private let downstreamLock = RecursiveLock()
        private let lock = Lock()
        private var state = State.initialized

        init(downstream: Downstream, parent: ObservableObjectPublisher) {
            self.parent = parent
            self.downstream = downstream
        }

        override func send() {
            lock.lock()
            let state = self.state
            lock.unlock()
            if state == .active {
                downstreamLock.lock()
                _ = downstream.receive()
                downstreamLock.unlock()
            }
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            if state == .initialized {
                state = .active
            }
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            state = .terminal
            lock.unlock()
            parent?.remove(self)
        }

        var description: String { "ObservableObjectPublisher" }

        var customMirror: Mirror {
            Mirror(self, children: CollectionOfOne<Mirror.Child>(("downstream", downstream)))
        }

        var playgroundDescription: Any { description }
    }
}
