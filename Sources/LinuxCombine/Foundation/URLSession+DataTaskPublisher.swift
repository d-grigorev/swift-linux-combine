import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

extension URLSession {

    /// Returns a publisher that wraps a URL session data task for a given URL.
    ///
    /// Official API (both overloads verified against Apple DocC).
    public func dataTaskPublisher(for url: URL) -> DataTaskPublisher {
        dataTaskPublisher(for: URLRequest(url: url))
    }

    /// Returns a publisher that wraps a URL session data task for a given
    /// URL request.
    public func dataTaskPublisher(for request: URLRequest) -> DataTaskPublisher {
        .init(request: request, session: self)
    }

    /// A publisher that delivers the results of performing URL session data
    /// tasks.
    ///
    /// Official API: `struct DataTaskPublisher` with
    /// `init(request:session:)` and `request`/`session` properties;
    /// `Output == (data: Data, response: URLResponse)`,
    /// `Failure == URLError`.
    ///
    /// Semantics (consulted reference): the task starts on the first
    /// `request(_:)`; a response arriving with zero outstanding demand is
    /// dropped; cancellation tears the task down.
    public struct DataTaskPublisher: LinuxCombine.Publisher {
        public typealias Output = (data: Data, response: URLResponse)
        public typealias Failure = URLError

        public let request: URLRequest
        public let session: URLSession

        public init(request: URLRequest, session: URLSession) {
            self.request = request
            self.session = session
        }

        public func receive<S: Subscriber>(subscriber: S)
        where S.Input == Output, S.Failure == Failure {
            subscriber.receive(subscription: Inner(parent: self, downstream: subscriber))
        }
    }
}

extension URLSession.DataTaskPublisher {

    private final class Inner<S: Subscriber>: Subscription,
        CustomStringConvertible, CustomReflectable, CustomPlaygroundDisplayConvertible
    where S.Input == (data: Data, response: URLResponse), S.Failure == URLError {
        private let lock = Lock()
        private var parent: URLSession.DataTaskPublisher?
        private var downstream: S?
        private var demand = Subscribers.Demand.none
        private var task: URLSessionDataTask?

        fileprivate init(parent: URLSession.DataTaskPublisher, downstream: S) {
            self.parent = parent
            self.downstream = downstream
        }

        func request(_ demand: Subscribers.Demand) {
            assert(demand != .none, "request(_:) demand must not be .none")
            lock.lock()
            guard let parent else {
                lock.unlock()
                return
            }
            if task == nil {
                task = parent.session.dataTask(
                    with: parent.request
                ) { [self] data, response, error in
                    handleResponse(data: data, response: response, error: error)
                }
            }
            self.demand += demand
            let task = self.task
            lock.unlock()
            task?.resume()
        }

        private func handleResponse(data: Data?, response: URLResponse?, error: Error?) {
            lock.lock()
            guard demand > 0, parent != nil, let downstream else {
                lock.unlock()
                return
            }
            terminateLocked()
            lock.unlock()
            switch (data, response, error) {
            case let (data, response?, nil):
                _ = downstream.receive((data ?? Data(), response))
                downstream.receive(completion: .finished)
            case let (_, _, error as URLError):
                downstream.receive(completion: .failure(error))
            default:
                downstream.receive(completion: .failure(URLError(.unknown)))
            }
        }

        func cancel() {
            lock.lock()
            guard parent != nil else {
                lock.unlock()
                return
            }
            let task = self.task
            terminateLocked()
            lock.unlock()
            task?.cancel()
        }

        private func terminateLocked() {
            parent = nil
            downstream = nil
            demand = .none
            task = nil
        }

        var description: String { "DataTaskPublisher" }

        var customMirror: Mirror {
            lock.lock()
            defer { lock.unlock() }
            let children: [Mirror.Child] = [
                ("task", task as Any),
                ("downstream", downstream as Any),
                ("parent", parent as Any),
                ("demand", demand),
            ]
            return Mirror(self, children: children)
        }

        var playgroundDescription: Any { description }
    }
}
