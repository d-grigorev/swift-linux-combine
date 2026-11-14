import XCTest
@testable import LinuxCombine

final class DemandTests: XCTestCase {

    // MARK: - Construction & properties

    func testSpecialDemands() {
        XCTAssertEqual(Subscribers.Demand.none.max, 0)
        XCTAssertNil(Subscribers.Demand.unlimited.max)
        XCTAssertEqual(Subscribers.Demand.max(5).max, 5)
        XCTAssertEqual(Subscribers.Demand.max(0), .none)
    }

    func testDescription() {
        XCTAssertEqual(Subscribers.Demand.none.description, "max(0)")
        XCTAssertEqual(Subscribers.Demand.max(3).description, "max(3)")
        XCTAssertEqual(Subscribers.Demand.unlimited.description, "unlimited")
    }

    // MARK: - Addition

    func testAddition() {
        let d = Subscribers.Demand.self
        XCTAssertEqual(d.max(3) + d.max(5), .max(8))
        XCTAssertEqual(d.none + d.max(1), .max(1))
        XCTAssertEqual(d.max(2) + d.none, .max(2))
        XCTAssertEqual(d.unlimited + d.max(3), .unlimited)
        XCTAssertEqual(d.max(3) + d.unlimited, .unlimited)
        XCTAssertEqual(d.unlimited + d.unlimited, .unlimited)
        XCTAssertEqual(d.max(3) + 4, .max(7))
        XCTAssertEqual(d.max(3) + (-1), .max(2))
        // unlimited absorbs integers
        XCTAssertEqual(d.unlimited + 5, .unlimited)

        var a = d.max(2)
        a += d.max(3)
        XCTAssertEqual(a, .max(5))
        a += d.unlimited
        XCTAssertEqual(a, .unlimited)

        var b = d.max(1)
        b += 4
        XCTAssertEqual(b, .max(5))
    }

    func testAdditionOverflowSaturatesToUnlimited() {
        let d = Subscribers.Demand.self
        XCTAssertEqual(d.max(Int.max) + d.max(1), .unlimited)
        var a = d.max(Int.max)
        a += 1
        XCTAssertEqual(a, .unlimited)
    }

    // MARK: - Multiplication

    func testMultiplication() {
        let d = Subscribers.Demand.self
        XCTAssertEqual(d.max(3) * 4, .max(12))
        XCTAssertEqual(d.unlimited * 3, .unlimited)
        XCTAssertEqual(d.none * 7, .none)
        var a = d.max(2)
        a *= 5
        XCTAssertEqual(a, .max(10))
    }

    // MARK: - Subtraction

    func testSubtractionSaturatesAtNone() {
        let d = Subscribers.Demand.self
        XCTAssertEqual(d.max(5) - d.max(3), .max(2))
        XCTAssertEqual(d.max(3) - d.max(5), .none)
        XCTAssertEqual(d.max(3) - d.unlimited, .none)
        XCTAssertEqual(d.unlimited - d.max(3), .unlimited)
        XCTAssertEqual(d.unlimited - d.unlimited, .unlimited)
        XCTAssertEqual(d.max(4) - 6, .none)
        XCTAssertEqual(d.max(4) - (-2), .max(6))
        XCTAssertEqual(d.unlimited - 2, .unlimited)

        var a = d.max(5)
        a -= d.max(9)
        XCTAssertEqual(a, .none)
        a = .unlimited
        a -= .max(3)
        XCTAssertEqual(a, .unlimited)
        var b = d.max(5)
        b -= 8
        XCTAssertEqual(b, .none)
    }

    // MARK: - Demand vs Demand comparison

    func testDemandOrdering() {
        let d = Subscribers.Demand.self
        XCTAssertLessThan(d.max(2), d.max(3))
        XCTAssertLessThanOrEqual(d.max(3), d.max(3))
        XCTAssertGreaterThan(d.max(3), d.max(2))
        XCTAssertGreaterThanOrEqual(d.max(3), d.max(3))

        // unlimited vs finite
        XCTAssertLessThan(d.max(Int.max), d.unlimited)
        XCTAssertLessThanOrEqual(d.max(Int.max), d.unlimited)
        XCTAssertGreaterThan(d.unlimited, d.max(Int.max))
        XCTAssertGreaterThanOrEqual(d.unlimited, d.max(Int.max))

        // unlimited vs unlimited: never ordered below / above, but <= and >= hold
        XCTAssertFalse(d.unlimited < d.unlimited)
        XCTAssertLessThanOrEqual(d.unlimited, d.unlimited)
        XCTAssertFalse(d.unlimited > d.unlimited)
        XCTAssertGreaterThanOrEqual(d.unlimited, d.unlimited)
        XCTAssertEqual(d.unlimited, d.unlimited)
        XCTAssertNotEqual(d.unlimited, d.max(3))
    }

    // MARK: - Demand vs Int comparison

    func testDemandIntComparisons() {
        let d = Subscribers.Demand.self

        XCTAssertTrue(d.max(3) == 3)
        XCTAssertFalse(d.max(3) == 4)
        XCTAssertTrue(3 == d.max(3))
        XCTAssertFalse(4 == d.max(3))
        XCTAssertFalse(d.max(3) != 3)
        XCTAssertTrue(d.max(3) != 4)
        XCTAssertTrue(4 != d.max(3))

        // unlimited never matches an integer
        XCTAssertFalse(d.unlimited == Int.max)
        XCTAssertTrue(d.unlimited != 0)
        XCTAssertFalse(0 == d.unlimited)
        XCTAssertTrue(0 != d.unlimited)

        // finite demand vs Int
        XCTAssertTrue(d.max(3) < 4)
        XCTAssertFalse(d.max(3) < 3)
        XCTAssertFalse(d.max(3) > 3)
        XCTAssertTrue(d.max(3) >= 3)
        XCTAssertFalse(d.max(3) <= 2)
        XCTAssertTrue(4 < d.max(5))
        XCTAssertFalse(5 < d.max(5))
        XCTAssertTrue(6 > d.max(5))
        XCTAssertFalse(5 > d.max(5))
        XCTAssertTrue(5 <= d.max(5))
        XCTAssertFalse(6 <= d.max(5))
        XCTAssertTrue(5 >= d.max(5))

        // unlimited vs Int: always greater
        XCTAssertTrue(d.unlimited > 4)
        XCTAssertFalse(d.unlimited < 4)
        XCTAssertFalse(d.unlimited <= 4)
        XCTAssertTrue(d.unlimited >= 4)
        XCTAssertTrue(4 < d.unlimited)
        XCTAssertFalse(6 > d.unlimited)
        XCTAssertFalse(6 >= d.unlimited)
        XCTAssertTrue(6 <= d.unlimited)
    }

    // MARK: - Hashable / Codable

    func testHashable() {
        var set = Set<Subscribers.Demand>()
        set.insert(.max(3))
        set.insert(.max(3))
        set.insert(.max(4))
        set.insert(.unlimited)
        XCTAssertEqual(set.count, 3)
    }

    func testCodableRoundtrip() throws {
        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        for demand: Subscribers.Demand in [.none, .max(7), .unlimited] {
            let data = try encoder.encode(demand)
            let back = try decoder.decode(Subscribers.Demand.self, from: data)
            XCTAssertEqual(back, demand)
        }
    }

    // MARK: - Official overload surface (compile-time drift check)

    func testOfficialOperatorOverloadsExist() {
        let d = Subscribers.Demand.self
        // Every official overload gets exercised once; if a signature drifts,
        // this file stops compiling. (Apple documents no (Int, Demand) + or -.)
        _ = d.max(1) + d.max(1)   // Demand + Demand
        _ = d.max(1) + 1          // Demand + Int
        _ = d.max(1) - d.max(1)   // Demand - Demand
        _ = d.max(1) - 1          // Demand - Int
        _ = d.max(1) * 2          // Demand * Int
        var a = d.max(1)
        a += d.max(1)
        a += 1
        a -= d.max(1)
        a -= 1
        a *= 2
        _ = d.max(1) == 1         // Demand == Int
        _ = 1 == d.max(1)         // Int == Demand
        _ = d.max(1) != 1
        _ = 1 != d.max(1)
        _ = d.max(1) < 1
        _ = 1 < d.max(1)
        _ = d.max(1) <= 1
        _ = 1 <= d.max(1)
        _ = d.max(1) > 1
        _ = 1 > d.max(1)
        _ = d.max(1) >= 1
        _ = 1 >= d.max(1)
        // Equatable (Demand, Demand) via synthesis
        _ = d.max(1) == d.max(1)
        _ = d.max(1) < d.max(2)
        _ = d.max(1) <= d.max(2)
        _ = d.max(2) > d.max(1)
        _ = d.max(2) >= d.max(1)
    }
}
