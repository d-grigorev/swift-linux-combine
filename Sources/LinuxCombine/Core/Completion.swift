extension Subscribers {

    /// The result of a publisher's task, delivered to a subscriber.
    ///
    /// Official API: cases `finished`, `failure(_:)`; `Equatable`, `Hashable`,
    /// `Sendable` conditional on `Failure`; `Encodable`/`Decodable`
    /// conditional on `Failure` with the custom wire format
    /// `{"success": true}` / `{"success": false, "error": …}`
    /// (verified against the Apple-validated parity suite).
    @frozen public enum Completion<Failure: Error> {
        /// The publisher terminated normally, with no failure.
        case finished
        /// The publisher terminated because of the provided failure.
        case failure(Failure)
    }
}

extension Subscribers.Completion: Equatable where Failure: Equatable {}
extension Subscribers.Completion: Hashable where Failure: Hashable {}
extension Subscribers.Completion: Sendable where Failure: Sendable {}

extension Subscribers.Completion {
    private enum CodingKeys: String, CodingKey {
        case success
        case error
    }
}

extension Subscribers.Completion: Encodable where Failure: Encodable {
    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .finished:
            try container.encode(true, forKey: .success)
        case .failure(let error):
            try container.encode(false, forKey: .success)
            try container.encode(error, forKey: .error)
        }
    }
}

extension Subscribers.Completion: Decodable where Failure: Decodable {
    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let success = try container.decode(Bool.self, forKey: .success)
        if success {
            self = .finished
        } else {
            self = .failure(try container.decode(Failure.self, forKey: .error))
        }
    }
}
