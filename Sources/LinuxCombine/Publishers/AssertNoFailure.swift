extension Publishers {

    /// A publisher that raises a fatal error upon receiving any failure,
    /// and otherwise republishes all received input.
    ///
    /// Official API: `struct AssertNoFailure<Upstream>` with
    /// `init(upstream:prefix:file:line:)`; `Failure == Never`.
    public struct AssertNoFailure<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Never

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// The string used at the beginning of the fatal error message.
        public let prefix: String

        /// The filename used in the error message.
        public let file: StaticString

        /// The line number used in the error message.
        public let line: UInt

        public init(upstream: Upstream, prefix: String, file: StaticString, line: UInt) {
            self.upstream = upstream
            self.prefix = prefix
            self.file = file
            self.line = line
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(
                downstream: subscriber,
                prefix: prefix,
                file: file,
                line: line
            ))
        }
    }
}

extension Publisher {
    /// Raises a fatal error when its upstream publisher fails, and
    /// otherwise republishes all received input.
    public func assertNoFailure(
        _ prefix: String = "",
        file: StaticString = #file,
        line: UInt = #line
    ) -> Publishers.AssertNoFailure<Self> {
        .init(upstream: self, prefix: prefix, file: file, line: line)
    }
}

extension Publishers.AssertNoFailure {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Never {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let prefix: String
        private let file: StaticString
        private let line: UInt

        let combineIdentifier = CombineIdentifier()

        init(downstream: Downstream, prefix: String, file: StaticString, line: UInt) {
            self.downstream = downstream
            self.prefix = prefix
            self.file = file
            self.line = line
        }

        func receive(subscription: any Subscription) {
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Input) -> Subscribers.Demand {
            downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Failure>) {
            switch completion {
            case .finished:
                downstream.receive(completion: .finished)
            case .failure(let error):
                let prefix = self.prefix.isEmpty ? "" : self.prefix + ": "
                fatalError("\(prefix)\(error)", file: file, line: line)
            }
        }

        var description: String { "AssertNoFailure" }

        var customMirror: Mirror {
            let children: [Mirror.Child] = [
                ("file", file),
                ("line", line),
                ("prefix", prefix),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
