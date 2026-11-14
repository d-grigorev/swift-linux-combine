import Foundation

// Apple's Combine conforms Foundation's top-level coders to its
// TopLevelEncoder/TopLevelDecoder protocols; mirrored here (the Codable
// operators themselves land with the coding batch).
extension JSONEncoder: TopLevelEncoder {}
extension JSONDecoder: TopLevelDecoder {}
extension PropertyListEncoder: TopLevelEncoder {}
extension PropertyListDecoder: TopLevelDecoder {}
