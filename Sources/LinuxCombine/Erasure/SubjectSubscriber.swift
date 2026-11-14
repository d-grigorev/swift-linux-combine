/// Internal relay that lets a `Subject` act as a subscriber of a publisher.
///
/// Semantics (resolved via the sanctioned ambiguity consult, OpenCombine's
/// `SubjectSubscriber`): when the upstream publisher delivers its
/// subscription, the relay sends *itself* — as a `Subscription` — into the
/// subject (`send(subscription:)`), so the subject's own subscribers' demand
/// flows back through `request(_:)` into the upstream subscription, and their
/// `cancel()` cancels upstream. Values flow forward into `send(_:)`.
///
/// The subject is held weakly: a relay never keeps its subject alive.
///
/// Description/mirror shape verified against the parity suite: prints
/// `"Subject"`.
internal final class SubjectSubscriber<Downstream: Subject>: Subscriber,
    Subscription,
    CustomStringConvertible,
    CustomReflectable,
    CustomPlaygroundDisplayConvertible
{

    private let lock = Lock()
    private weak var subject: Downstream?
    private var upstream: (any Subscription)?

    internal init(_ subject: Downstream) {
        self.subject = subject
    }

    // MARK: Subscriber

    internal func receive(subscription: any Subscription) {
        lock.lock()
        guard upstream == nil, subject != nil else {
            lock.unlock()
            return
        }
        upstream = subscription
        let subject = self.subject
        lock.unlock()
        subject?.send(subscription: self)
    }

    internal func receive(_ input: Downstream.Output) -> Subscribers.Demand {
        lock.lock()
        guard let subject, upstream != nil else {
            lock.unlock()
            return .none
        }
        lock.unlock()
        subject.send(input)
        return .none
    }

    internal func receive(completion: Subscribers.Completion<Downstream.Failure>) {
        lock.lock()
        guard let subject, upstream != nil else {
            lock.unlock()
            return
        }
        self.subject = nil
        lock.unlock()
        subject.send(completion: completion)
    }

    // MARK: Subscription

    internal func request(_ demand: Subscribers.Demand) {
        lock.lock()
        guard let upstream else {
            lock.unlock()
            return
        }
        lock.unlock()
        upstream.request(demand)
    }

    internal func cancel() {
        lock.lock()
        guard subject != nil, let upstream else {
            lock.unlock()
            return
        }
        self.subject = nil
        self.upstream = nil
        lock.unlock()
        upstream.cancel()
    }

    // MARK: Description & reflection

    internal var description: String { "Subject" }

    internal var playgroundDescription: Any { description }

    internal var customMirror: Mirror {
        lock.lock()
        defer { lock.unlock() }
        let children: [Mirror.Child] = [
            ("downstreamSubject", subject as Any),
            ("upstreamSubscription", upstream as Any),
            ("subject", subject as Any),
        ]
        return Mirror(self, children: children)
    }
}
