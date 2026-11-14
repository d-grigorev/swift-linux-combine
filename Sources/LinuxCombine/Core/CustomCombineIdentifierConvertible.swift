/// A type that provides a stable Combine identifier.
///
/// Official API: property `combineIdentifier`.
/// Classes get a default implementation backed by their object identity;
/// value types must store their own identifier so it stays stable.
public protocol CustomCombineIdentifierConvertible {
    var combineIdentifier: CombineIdentifier { get }
}

extension CustomCombineIdentifierConvertible where Self: AnyObject {
    /// The default for class instances: identity is the object itself.
    public var combineIdentifier: CombineIdentifier {
        CombineIdentifier(self)
    }
}
