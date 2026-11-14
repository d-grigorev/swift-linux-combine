/// A type-erasing publisher.
///
/// Official API verified against Apple DocC: `@frozen struct
/// AnyPublisher<Output, Failure>` conforming to `Publisher`,
/// `CustomStringConvertible`, `CustomPlaygroundDisplayConvertible`
/// (deliberately **not** `CustomReflectable`); single init `init(_:)`.
///
/// Layout: a single class box (mirrors Apple's implementation — double
/// erasure shares one box, which the parity suite verifies at the
/// bit-pattern level). `description` is the fixed string "AnyPublisher".
@frozen public struct AnyPublisher<Output, Failure: Error>:
    CustomStringConvertible,
    CustomPlaygroundDisplayConvertible
{
    @usableFromInline
    internal let box: PublisherBoxBase<Output, Failure>

    /// Wraps an existing publisher with a type eraser.
    public init<P: Publisher>(_ publisher: P)
    where P.Output == Output, P.Failure == Failure {
        // If this has already been boxed, avoid boxing again.
        if let erased = publisher as? AnyPublisher<Output, Failure> {
            box = erased.box
        } else {
            box = PublisherBox(base: publisher)
        }
    }

    public var description: String { "AnyPublisher" }

    public var playgroundDescription: Any { description }
}

extension AnyPublisher: Publisher {
    /// Attaches the specified subscriber to the wrapped publisher, passed
    /// through unerased.
    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        box.receive(subscriber: subscriber)
    }
}

/// Type-erasing base class for publisher boxes; concrete subclass is
/// generic over the underlying publisher.
@usableFromInline
internal class PublisherBoxBase<Output, Failure: Error>: Publisher {
    internal init() {}

    @usableFromInline
    internal func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        fatalError("abstract method")
    }
}

internal final class PublisherBox<P: Publisher>
    : PublisherBoxBase<P.Output, P.Failure>
{
    internal let base: P

    internal init(base: P) {
        self.base = base
        super.init()
    }

    override internal func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        base.receive(subscriber: subscriber)
    }
}
