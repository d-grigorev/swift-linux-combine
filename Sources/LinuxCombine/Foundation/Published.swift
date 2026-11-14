import Foundation

/// A type that publishes a property marked with an attribute.
///
/// Official API verified against Apple DocC: `@propertyWrapper struct
/// Published<Value>` with `init(initialValue:)`, `init(wrappedValue:)`,
/// `projectedValue` (mutating get/set) and nested `struct Publisher`.
///
/// Semantics (consulted reference + official docs): values are published in
/// `willSet` — subscribers see the new value *before* the property itself
/// changes; the property storage lives in a reference box so class-property
/// access goes through the `_enclosingInstance` subscript; the first
/// `$`-access (or first write) converts the stored value into a publisher.
@available(swift, introduced: 5.1)
@propertyWrapper
public struct Published<Value> {

    /// A publisher for properties marked with the `@Published` attribute.
    public struct Publisher: LinuxCombine.Publisher {
        public typealias Output = Value
        public typealias Failure = Never

        fileprivate let subject: PublishedSubject<Value>

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Value, S.Failure == Never {
            subject.subscribe(subscriber)
        }

        fileprivate init(_ output: Output) {
            subject = .init(output)
        }
    }

    private enum Storage {
        case value(Value)
        case publisher(Publisher)
    }

    @propertyWrapper
    private final class Box {
        var wrappedValue: Storage

        init(wrappedValue: Storage) {
            self.wrappedValue = wrappedValue
        }
    }

    @Box private var storage: Storage

    internal var objectWillChange: ObservableObjectPublisher? {
        get {
            switch storage {
            case .value:
                return nil
            case .publisher(let publisher):
                return publisher.subject.objectWillChange
            }
        }
        nonmutating set {
            getPublisher().subject.objectWillChange = newValue
        }
    }

    /// Creates the published instance with an initial wrapped value.
    public init(initialValue: Value) {
        self.init(wrappedValue: initialValue)
    }

    /// Creates the published instance with an initial value.
    public init(wrappedValue: Value) {
        _storage = Box(wrappedValue: .value(wrappedValue))
    }

    /// The property for which this instance exposes a publisher (`$`).
    public var projectedValue: Publisher {
        mutating get {
            getPublisher()
        }
        set {
            switch storage {
            case .value(let value):
                storage = .publisher(Publisher(value))
            case .publisher:
                break
            }
        }
    }

    /// Converts the stored value into a publisher on first access.
    internal func getPublisher() -> Publisher {
        switch storage {
        case .value(let value):
            let publisher = Publisher(value)
            storage = .publisher(publisher)
            return publisher
        case .publisher(let publisher):
            return publisher
        }
    }

    @available(*, unavailable, message: """
               @Published is only available on properties of classes
               """)
    public var wrappedValue: Value {
        get { fatalError() }
        set { fatalError() }
    }

    public static subscript<EnclosingSelf: AnyObject>(
        _enclosingInstance object: EnclosingSelf,
        wrapped wrappedKeyPath: ReferenceWritableKeyPath<EnclosingSelf, Value>,
        storage storageKeyPath: ReferenceWritableKeyPath<EnclosingSelf, Published<Value>>
    ) -> Value {
        get {
            switch object[keyPath: storageKeyPath].storage {
            case .value(let value):
                return value
            case .publisher(let publisher):
                return publisher.subject.value
            }
        }
        set {
            switch object[keyPath: storageKeyPath].storage {
            case .value:
                object[keyPath: storageKeyPath].storage = .publisher(Publisher(newValue))
            case .publisher(let publisher):
                publisher.subject.value = newValue
            }
        }
    }
}

extension Publisher where Failure == Never {
    /// Republishes elements received from a publisher, by assigning them to
    /// a property marked with the `@Published` attribute.
    ///
    /// The subscription is managed by the `Published` instance — no
    /// `AnyCancellable` to hold (cancels when the publisher storage dies).
    public func assign(to published: inout Published<Output>.Publisher) {
        subscribe(PublishedSubscriber(published.subject))
    }
}

// MARK: - Internal subject/subscriber for @Published

/// The subject backing a `@Published` property: a `CurrentValueSubject`
/// variant that additionally fires `objectWillChange` before each send and
/// can never complete.
internal final class PublishedSubject<Output>: Subject {
    internal typealias Failure = Never

    private let lock = Lock()
    private var downstreams: [ConduitBase<Output, Never>] = []
    private var currentValue: Output
    private var upstreamSubscriptions: [any Subscription] = []
    private var changePublisher: ObservableObjectPublisher?

    internal var value: Output {
        get {
            lock.withLock { currentValue }
        }
        set {
            send(newValue)
        }
    }

    internal var objectWillChange: ObservableObjectPublisher? {
        get { lock.withLock { changePublisher } }
        set { lock.withLock { changePublisher = newValue } }
    }

    internal init(_ value: Output) {
        self.currentValue = value
    }

    deinit {
        for subscription in upstreamSubscriptions {
            subscription.cancel()
        }
    }

    internal func send(subscription: any Subscription) {
        lock.lock()
        upstreamSubscriptions.append(subscription)
        lock.unlock()
        subscription.request(.unlimited)
    }

    internal func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Never {
        lock.lock()
        let conduit = Conduit(parent: self, downstream: subscriber)
        downstreams.append(conduit)
        lock.unlock()
        subscriber.receive(subscription: conduit)
    }

    internal func send(_ input: Output) {
        lock.lock()
        let downstreams = self.downstreams
        let changePublisher = self.changePublisher
        lock.unlock()
        // objectWillChange fires BEFORE the new value is stored/delivered.
        changePublisher?.send()
        for conduit in downstreams {
            conduit.offer(input)
        }
        lock.lock()
        currentValue = input
        lock.unlock()
    }

    internal func send(completion: Subscribers.Completion<Never>) {
        fatalError("unreachable")
    }

    fileprivate func disassociate(_ conduit: ConduitBase<Output, Never>) {
        lock.lock()
        downstreams.removeAll { $0 === conduit }
        lock.unlock()
    }
}

extension PublishedSubject {

    private final class Conduit<Downstream: Subscriber>
        : ConduitBase<Output, Never>, Subscription,
          CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Never {
        private var parent: PublishedSubject?
        private var downstream: Downstream?
        private var demand = Subscribers.Demand.none
        private var deliveredCurrentValue = false
        private let lock = Lock()
        private let downstreamLock = RecursiveLock()

        fileprivate init(parent: PublishedSubject, downstream: Downstream) {
            self.parent = parent
            self.downstream = downstream
        }

        override func offer(_ output: Output) {
            lock.lock()
            guard demand > 0, let downstream else {
                deliveredCurrentValue = false
                lock.unlock()
                return
            }
            demand -= 1
            deliveredCurrentValue = true
            lock.unlock()
            downstreamLock.lock()
            let newDemand = downstream.receive(output)
            downstreamLock.unlock()
            guard newDemand > 0 else { return }
            lock.lock()
            demand += newDemand
            lock.unlock()
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            guard let downstream else {
                lock.unlock()
                return
            }
            if deliveredCurrentValue {
                self.demand += demand
                lock.unlock()
                return
            }
            // Owed the current value.
            self.demand += demand
            deliveredCurrentValue = true
            if let currentValue = parent?.value {
                self.demand -= 1
                lock.unlock()
                downstreamLock.lock()
                let newDemand = downstream.receive(currentValue)
                downstreamLock.unlock()
                guard newDemand > 0 else { return }
                lock.lock()
                self.demand += newDemand
                lock.unlock()
                return
            }
            lock.unlock()
        }

        func cancel() {
            lock.lock()
            guard downstream != nil else {
                lock.unlock()
                return
            }
            downstream = nil
            let parent = self.parent
            self.parent = nil
            lock.unlock()
            parent?.disassociate(self)
        }

        var description: String { "PublishedSubject" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("parent", parent as Any),
                ("downstream", downstream as Any),
                ("demand", demand),
                ("subject", parent as Any),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}

/// Forwards `assign(to:)`'s upstream into a `Published` storage; holds the
/// subject weakly (the `Published` instance owns its own lifecycle).
internal struct PublishedSubscriber<Value>: Subscriber {
    internal typealias Input = Value
    internal typealias Failure = Never

    internal let combineIdentifier = CombineIdentifier()

    private weak var subject: PublishedSubject<Value>?

    internal init(_ subject: PublishedSubject<Value>) {
        self.subject = subject
    }

    internal func receive(subscription: any Subscription) {
        subject?.send(subscription: subscription)
    }

    internal func receive(_ input: Value) -> Subscribers.Demand {
        subject?.send(input)
        return .none
    }

    internal func receive(completion: Subscribers.Completion<Never>) {}
}
