/// A type that transforms a top-level instance into a serialized form.
///
/// Official API verified against Apple DocC: `protocol TopLevelEncoder`
/// with `associatedtype Output` and `encode(_:)`.
public protocol TopLevelEncoder {
    associatedtype Output

    /// Encodes a top-level instance into the encoder's output format.
    func encode<Value: Encodable>(_ value: Value) throws -> Output
}

/// A type that transforms a serialized form back into a top-level instance.
///
/// Official API verified against Apple DocC: `protocol TopLevelDecoder`
/// with `associatedtype Input` and `decode(_:from:)`.
public protocol TopLevelDecoder {
    associatedtype Input

    /// Decodes a top-level instance from the given serialized input.
    func decode<Value: Decodable>(_ type: Value.Type, from: Input) throws -> Value
}
