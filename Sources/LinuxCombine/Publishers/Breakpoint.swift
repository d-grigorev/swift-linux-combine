#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

extension Publishers {

    /// A publisher that raises a debugger signal when a provided closure
    /// needs to stop the process in the debugger.
    ///
    /// Official API: `struct Breakpoint<Upstream>` with the four-parameter
    /// init; the upstream subscription is passed through directly (the
    /// inner is a plain subscriber).
    public struct Breakpoint<Upstream: Publisher>: Publisher {
        public typealias Output = Upstream.Output
        public typealias Failure = Upstream.Failure

        /// The publisher from which this publisher receives elements.
        public let upstream: Upstream

        /// A closure that executes when the publisher receives a
        /// subscription, and can raise a debugger signal by returning `true`.
        public let receiveSubscription: ((any Subscription) -> Bool)?

        /// A closure that executes when the publisher receives output from
        /// the upstream publisher, and can raise a debugger signal by
        /// returning `true`.
        public let receiveOutput: ((Upstream.Output) -> Bool)?

        /// A closure that executes when the publisher receives completion,
        /// and can raise a debugger signal by returning `true`.
        public let receiveCompletion: ((Subscribers.Completion<Upstream.Failure>) -> Bool)?

        public init(
            upstream: Upstream,
            receiveSubscription: ((any Subscription) -> Bool)? = nil,
            receiveOutput: ((Upstream.Output) -> Bool)? = nil,
            receiveCompletion: ((Subscribers.Completion<Failure>) -> Bool)? = nil
        ) {
            self.upstream = upstream
            self.receiveSubscription = receiveSubscription
            self.receiveOutput = receiveOutput
            self.receiveCompletion = receiveCompletion
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            upstream.subscribe(Inner(self, downstream: subscriber))
        }
    }
}

extension Publisher {
    /// Raises a debugger signal when a provided closure needs to stop the
    /// process in the debugger.
    public func breakpoint(
        receiveSubscription: ((any Subscription) -> Bool)? = nil,
        receiveOutput: ((Output) -> Bool)? = nil,
        receiveCompletion: ((Subscribers.Completion<Failure>) -> Bool)? = nil
    ) -> Publishers.Breakpoint<Self> {
        .init(
            upstream: self,
            receiveSubscription: receiveSubscription,
            receiveOutput: receiveOutput,
            receiveCompletion: receiveCompletion
        )
    }

    /// Raises a debugger signal upon receiving a failure.
    public func breakpointOnError() -> Publishers.Breakpoint<Self> {
        breakpoint(receiveCompletion: { completion in
            switch completion {
            case .finished:
                return false
            case .failure:
                return true
            }
        })
    }
}

extension Publishers.Breakpoint {

    private struct Inner<Downstream: Subscriber>: Subscriber,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where Downstream.Input == Upstream.Output, Downstream.Failure == Upstream.Failure {
        typealias Input = Upstream.Output
        typealias Failure = Upstream.Failure

        private let downstream: Downstream
        private let breakpoint: Publishers.Breakpoint<Upstream>

        let combineIdentifier = CombineIdentifier()

        init(_ breakpoint: Publishers.Breakpoint<Upstream>, downstream: Downstream) {
            self.downstream = downstream
            self.breakpoint = breakpoint
        }

        func receive(subscription: any Subscription) {
            if breakpoint.receiveSubscription?(subscription) == true {
                stopInDebugger()
            }
            downstream.receive(subscription: subscription)
        }

        func receive(_ input: Upstream.Output) -> Subscribers.Demand {
            if breakpoint.receiveOutput?(input) == true {
                stopInDebugger()
            }
            return downstream.receive(input)
        }

        func receive(completion: Subscribers.Completion<Upstream.Failure>) {
            if breakpoint.receiveCompletion?(completion) == true {
                stopInDebugger()
            }
            downstream.receive(completion: completion)
        }

        var description: String { "Breakpoint" }

        var customMirror: Mirror { Mirror(self, children: EmptyCollection()) }

        var playgroundDescription: Any { description }

        private func stopInDebugger() {
            raise(SIGTRAP)
        }
    }
}
