import XCTest
@testable import LinuxCombine

final class IdentifierTests: XCTestCase {

    private final class Token: CustomCombineIdentifierConvertible {}

    func testFreshIdentifiersAreUnique() {
        let a = CombineIdentifier()
        let b = CombineIdentifier()
        XCTAssertNotEqual(a, b)
        XCTAssertNotEqual(a.hashValue, b.hashValue)  // probabilistically distinct
    }

    func testObjectIdentifiersTrackIdentity() {
        let token = Token()
        let a = CombineIdentifier(token)
        let b = CombineIdentifier(token)
        XCTAssertEqual(a, b)
        XCTAssertEqual(a.description, b.description)

        let other = Token()
        XCTAssertNotEqual(a, CombineIdentifier(other))
        XCTAssertNotEqual(CombineIdentifier(token), CombineIdentifier(other))
    }

    func testObjectAndFreshIdentifiersNeverCollide() {
        let token = Token()
        let objectID = CombineIdentifier(token)
        var freshIDs = Set<CombineIdentifier>()
        for _ in 0..<100 {
            freshIDs.insert(CombineIdentifier())
        }
        XCTAssertFalse(freshIDs.contains(objectID))
    }

    func testClassDefaultCombineIdentifierIsObjectIdentity() {
        let token = Token()
        let first = token as any CustomCombineIdentifierConvertible
        let second = token as any CustomCombineIdentifierConvertible
        XCTAssertEqual(first.combineIdentifier, second.combineIdentifier)
        XCTAssertNotEqual(
            first.combineIdentifier,
            CombineIdentifier(Token())
        )
    }

    func testHashableSetSemantics() {
        let token = Token()
        let id = CombineIdentifier(token)
        var set = Set<CombineIdentifier>()
        set.insert(id)
        set.insert(CombineIdentifier(token))
        set.insert(CombineIdentifier())
        XCTAssertEqual(set.count, 2)
    }
}
