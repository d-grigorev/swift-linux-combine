/// A publisher that allows for recording a series of inputs and a completion
/// for later playback to each subscriber.
///
/// Official API verified against Apple DocC: `struct Record<Output, Failure>`
/// with `init(output:completion:)`, `init(record:)`, `init(recording:)`,
/// `let recording`, nested `Recording` (itself `Codable` where
/// `Output`/`Failure` are), plus `Recording`'s `output`, `completion`,
/// `receive(_:)`, `receive(completion:)`, `encode(into:)`.
///
/// Playback: an empty recording completes immediately (not demand-gated);
/// otherwise the replayed values — and the final completion — are
/// demand-driven.
public struct Record<Output, Failure: Error>: Publisher {

    /// The recorded output and completion.
    public let recording: Recording

    public init(record: (inout Recording) -> Void) {
        var recording = Recording()
        record(&recording)
        self.init(recording: recording)
    }

    public init(recording: Recording) {
        self.recording = recording
    }

    public init(output: [Output], completion: Subscribers.Completion<Failure>) {
        self.init(recording: Recording(output: output, completion: completion))
    }

    public func receive<S: Subscriber>(subscriber: S)
    where S.Input == Output, S.Failure == Failure {
        if recording.output.isEmpty {
            subscriber.receive(subscription: Subscriptions.empty)
            subscriber.receive(completion: recording.completion)
        } else {
            let inner = Inner(
                downstream: subscriber,
                sequence: recording.output,
                completion: recording.completion
            )
            subscriber.receive(subscription: inner)
        }
    }

    /// A recorded sequence of outputs, followed by a completion value.
    public struct Recording {

        public typealias Input = Output

        private enum State {
            case input
            case complete
        }

        private var state: State

        /// The output which will be sent to a `Subscriber`.
        public private(set) var output: [Output]

        /// The completion which will be sent to a `Subscriber`.
        public private(set) var completion: Subscribers.Completion<Failure>

        /// Set up a recording in a state ready to receive output.
        public init() {
            state = .input
            output = []
            completion = .finished
        }

        /// Set up a complete recording with the specified output and completion.
        public init(
            output: [Output],
            completion: Subscribers.Completion<Failure> = .finished
        ) {
            state = .complete
            self.output = output
            self.completion = completion
        }

        /// Add an output to the recording.
        ///
        /// A `precondition` failure is raised if output is added after
        /// adding completion.
        public mutating func receive(_ input: Input) {
            precondition(state == .input,
                         "Receiving values after completion is not allowed")
            output.append(input)
        }

        /// Add a completion to the recording.
        ///
        /// A `precondition` failure is raised if more than one completion
        /// is added.
        public mutating func receive(completion: Subscribers.Completion<Failure>) {
            precondition(state == .input,
                         "Receiving completion more than once is not allowed")
            self.completion = completion
            state = .complete
        }
    }
}

extension Record: Codable where Output: Codable, Failure: Codable {}

extension Record.Recording: Codable where Output: Codable, Failure: Codable {

    private enum CodingKeys: String, CodingKey {
        case output
        case completion
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let output = try container.decode([Output].self, forKey: .output)
        let completion = try container.decode(
            Subscribers.Completion<Failure>.self,
            forKey: .completion
        )
        self.init(output: output, completion: completion)
    }

    public func encode(into encoder: any Encoder) throws {
        try encode(to: encoder)
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(output, forKey: .output)
        try container.encode(completion, forKey: .completion)
    }
}

extension Record {

    /// Demand-driven replay of the recorded elements.
    private final class Inner<Downstream: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Output, Downstream.Failure == Failure {

        private var sequence: [Output]?
        private let completion: Subscribers.Completion<Failure>
        private var downstream: Downstream?
        private var iterator: IndexingIterator<[Output]>
        private var next: Output?
        private var pendingDemand = Subscribers.Demand.none
        private var recursion = false
        private let lock = Lock()

        fileprivate init(
            downstream: Downstream,
            sequence: [Output],
            completion: Subscribers.Completion<Failure>
        ) {
            self.sequence = sequence
            self.completion = completion
            self.downstream = downstream
            iterator = sequence.makeIterator()
            next = iterator.next()
        }

        func request(_ demand: Subscribers.Demand) {
            lock.lock()
            guard downstream != nil else {
                lock.unlock()
                return
            }
            pendingDemand += demand
            if recursion {
                lock.unlock()
                return
            }

            while let downstream, pendingDemand > 0 {
                if let current = next {
                    pendingDemand -= 1
                    let next = iterator.next()
                    recursion = true
                    lock.unlock()
                    let additionalDemand = downstream.receive(current)
                    lock.lock()
                    recursion = false
                    pendingDemand += additionalDemand
                    self.next = next
                }

                if next == nil {
                    self.downstream = nil
                    sequence = nil
                    lock.unlock()
                    downstream.receive(completion: completion)
                    return
                }
            }

            lock.unlock()
        }

        func cancel() {
            lock.lock()
            downstream = nil
            sequence = nil
            lock.unlock()
        }

        var description: String {
            lock.lock()
            defer { lock.unlock() }
            return sequence.map { $0.description } ?? "Cancelled Events"
        }

        var playgroundDescription: Any { description }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("sequence", sequence ?? [Output]()),
                ("completion", completion),
            ]
            return Mirror(self, children: children)
        }
    }
}
