/// Type-erased handle a subject uses to push elements into a per-subscriber
/// conduit (the concrete conduit is generic over its downstream subscriber).
///
/// Mirrors the internal `ConduitBase` pattern: the subject's storage is
/// heterogeneous across downstreams, while each concrete conduit also
/// conforms to `Subscription` for its own subscriber.
internal class ConduitBase<Output, Failure: Error> {
    internal init() {}

    /// Offers one element; the concrete conduit applies its demand accounting
    /// (subjects *drop* elements when demand is exhausted — documented
    /// PassthroughSubject behavior).
    internal func offer(_ output: Output) {
        fatalError("abstract method")
    }

    /// Delivers the terminal completion.
    internal func finish(completion: Subscribers.Completion<Failure>) {
        fatalError("abstract method")
    }
}
