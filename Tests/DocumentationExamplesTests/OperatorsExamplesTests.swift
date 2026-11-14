import XCTest
import LinuxCombine

/// Executable counterparts of every `<!-- example: operators/… -->`
/// snippet in docs/OPERATORS.md (and the Russian edition).
final class OperatorsExamplesTests: XCTestCase {

    // MARK: - Filtering

    func testOperatorsFilter() {
        var kept: [Int] = []
        let c1 = [1, 2, 2, 3, 3, 3].publisher
            .filter { $0 % 2 == 0 }
            .removeDuplicates()
            .sink { kept.append($0) }
        defer { c1.cancel() }
        XCTAssertEqual(kept, [2])

        var mapped: [String] = []
        let c2 = [1, 4, 9].publisher
            .compactMap { $0 == 4 ? "four" : nil }
            .sink { mapped.append($0) }
        defer { c2.cancel() }
        XCTAssertEqual(mapped, ["four"])
    }

    func testOperatorsRangeSlicing() {
        var kept: [Int] = []
        let c1 = [1, 2, 3, 4, 5].publisher
            .dropFirst(2)
            .prefix(2)
            .sink { kept.append($0) }
        defer { c1.cancel() }
        XCTAssertEqual(kept, [3, 4])

        var one: [Int] = []
        let c2 = [10, 20, 30].publisher.output(at: 1).sink { one.append($0) }
        defer { c2.cancel() }
        XCTAssertEqual(one, [20])
    }

    // MARK: - Transforming

    func testOperatorsMapFamily() {
        struct Person { let name: String; let age: Int }
        let people = [Person(name: "Ann", age: 30),
                      Person(name: "Bo", age: 4)].publisher

        var names: [String] = []
        let c1 = people.map(\.name).sink { names.append($0) }
        defer { c1.cancel() }
        XCTAssertEqual(names, ["Ann", "Bo"])

        var tuples: [(String, Int)] = []
        let c2 = people.map(\.name, \.age).sink { tuples.append($0) }
        defer { c2.cancel() }
        XCTAssertEqual(tuples.count, 2)
        XCTAssertEqual(tuples[0].0, "Ann")
        XCTAssertEqual(tuples[0].1, 30)
        XCTAssertEqual(tuples[1].0, "Bo")
        XCTAssertEqual(tuples[1].1, 4)

        var scanned: [Int] = []
        let c3 = [1, 2, 3].publisher.scan(0, +).sink { scanned.append($0) }
        defer { c3.cancel() }
        XCTAssertEqual(scanned, [1, 3, 6])
    }

    // MARK: - Collecting

    func testOperatorsCollect() {
        var arrays: [[Int]] = []
        let c1 = [1, 2, 3].publisher.collect().sink { arrays.append($0) }
        defer { c1.cancel() }
        XCTAssertEqual(arrays, [[1, 2, 3]])

        var chunks: [[Int]] = []
        let c2 = [1, 2, 3, 4, 5].publisher.collect(2).sink { chunks.append($0) }
        defer { c2.cancel() }
        XCTAssertEqual(chunks, [[1, 2], [3, 4], [5]])
    }

    // MARK: - Reducing & selection

    func testOperatorsReduce() {
        var sum = 0
        let c1 = [1, 2, 3].publisher.reduce(0, +).sink { sum = $0 }
        defer { c1.cancel() }
        XCTAssertEqual(sum, 6)

        var maximum = 0
        let c2 = [7, 3, 9].publisher.max().sink { maximum = $0 }
        defer { c2.cancel() }
        XCTAssertEqual(maximum, 9)
    }

    func testOperatorsSelection() {
        var allEven = false
        let c1 = [2, 4, 6].publisher.allSatisfy { $0 % 2 == 0 }
            .sink { allEven = $0 }
        defer { c1.cancel() }
        XCTAssertTrue(allEven)

        var hasSeven = false
        let c2 = [1, 7, 3].publisher.contains(7).sink { hasSeven = $0 }
        defer { c2.cancel() }
        XCTAssertTrue(hasSeven)

        var total = 0
        let c3 = [1, 2, 3, 4].publisher.count().sink { total = $0 }
        defer { c3.cancel() }
        XCTAssertEqual(total, 4)
    }

    // MARK: - Combining

    func testOperatorsCombineLatest() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<String, Never>()
        var pairs: [String] = []

        let cancellable = a.combineLatest(b) { num, str in "\(num)\(str)" }
            .sink { pairs.append($0) }
        defer { cancellable.cancel() }

        a.send(1)
        b.send("x")     // both have values now → "1x"
        b.send("y")     // → "1y"
        a.send(2)       // → "2y"

        XCTAssertEqual(pairs, ["1x", "1y", "2y"])
    }

    func testOperatorsZip() {
        let names = ["Ann", "Bo", "Cy"].publisher
        let ages = [30, 40, 50].publisher
        var people: [String] = []

        let cancellable = names.zip(ages) { $0 + " is \($1)" }
            .sink { people.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(people, ["Ann is 30", "Bo is 40", "Cy is 50"])
    }

    func testOperatorsMerge() {
        let a = PassthroughSubject<Int, Never>()
        let b = PassthroughSubject<Int, Never>()
        var seen: [Int] = []

        let cancellable = Publishers.Merge(a, b).sink { seen.append($0) }
        defer { cancellable.cancel() }

        a.send(1)
        b.send(2)
        a.send(3)

        XCTAssertEqual(seen, [1, 2, 3])
    }

    // MARK: - Sequencing

    func testOperatorsAppendPrepend() {
        var seen: [Int] = []
        let cancellable = [1, 2].publisher
            .append([3, 4])
            .prepend(0)
            .sink { seen.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(seen, [0, 1, 2, 3, 4])
    }

    func testOperatorsFlatmapSwitch() {
        let outer = PassthroughSubject<Int, Never>()
        var seen: [String] = []

        let flat = outer
            .flatMap { value in ["a\(value)", "b\(value)"].publisher }
            .sink { seen.append($0) }
        defer { flat.cancel() }

        outer.send(1)
        XCTAssertEqual(seen, ["a1", "b1"])

        // switchToLatest keeps only the newest inner stream:
        let outer2 = PassthroughSubject<PassthroughSubject<Int, Never>, Never>()
        var switched: [Int] = []
        let latest = outer2.switchToLatest().sink { switched.append($0) }
        defer { latest.cancel() }

        let inner1 = PassthroughSubject<Int, Never>()
        let inner2 = PassthroughSubject<Int, Never>()
        outer2.send(inner1)
        inner1.send(1)      // switched == [1]
        outer2.send(inner2) // inner1 is cancelled
        inner1.send(9)      // ignored
        inner2.send(2)      // switched == [1, 2]

        XCTAssertEqual(switched, [1, 2])
    }

    // MARK: - Error handling

    func testOperatorsTryCatch() {
        struct Oops: Error {}

        var seen: [Int] = []
        let cancellable = [1, 2, 3].publisher
            .tryMap { value -> Int in
                if value == 2 { throw Oops() }
                return value
            }
            .catch { _ in Just(-1) }
            .sink { seen.append($0) }
        defer { cancellable.cancel() }

        XCTAssertEqual(seen, [1, -1])
    }

    func testOperatorsReplaceErrorEmpty() {
        struct Oops: Error {}

        var seen: [Int] = []
        let c1 = Fail<Int, Error>(error: Oops())
            .replaceError(with: 0)
            .sink { seen.append($0) }
        defer { c1.cancel() }
        XCTAssertEqual(seen, [0])

        var got = 0
        let empty = [Int]().publisher.replaceEmpty(with: -1)
        let c2 = empty.sink { got = $0 }
        defer { c2.cancel() }
        XCTAssertEqual(got, -1)
    }

    // MARK: - Debugging

    func testOperatorsPrintHandleEvents() {
        var hookLog: [String] = []
        let subject = PassthroughSubject<Int, Never>()
        var seen: [Int] = []

        let cancellable = subject
            .handleEvents(receiveOutput: { hookLog.append("out \($0)") },
                          receiveCancel: { hookLog.append("cancelled") })
            .sink { seen.append($0) }

        subject.send(4)
        XCTAssertEqual(hookLog, ["out 4"])
        XCTAssertEqual(seen, [4])

        cancellable.cancel()
        XCTAssertEqual(hookLog, ["out 4", "cancelled"])
    }

    // MARK: - Connectable

    func testOperatorsShare() {
        let source = PassthroughSubject<Int, Never>()
        let shared = source.share()
        var first: [Int] = []
        var second: [Int] = []

        let c1 = shared.sink { first.append($0) }
        defer { c1.cancel() }
        let c2 = shared.sink { second.append($0) }
        defer { c2.cancel() }

        source.send(1)
        source.send(2)

        XCTAssertEqual(first, [1, 2])
        XCTAssertEqual(second, [1, 2])
    }

    func testOperatorsMakeConnectable() {
        let subject = PassthroughSubject<Int, Never>()
        let connectable = subject.makeConnectable()
        var seen: [Int] = []
        let cancellable = connectable.sink { seen.append($0) }
        defer { cancellable.cancel() }

        subject.send(1)      // not connected → dropped
        let connection = connectable.connect()
        subject.send(2)
        XCTAssertEqual(seen, [2])
        connection.cancel()
    }

    // MARK: - Coding

    func testOperatorsEncodeDecode() {
        struct Article: Codable, Equatable {
            let title: String
            let year: Int
        }
        let article = Article(title: "A", year: 2026)

        var encoded: [Data] = []
        let c1 = [article].publisher
            .encode(encoder: JSONEncoder())          // Failure == Error
            .sink(receiveCompletion: { _ in },
                  receiveValue: { encoded.append($0) })
        defer { c1.cancel() }
        XCTAssertEqual(encoded.count, 1)
        XCTAssertFalse(encoded[0].isEmpty)

        var decoded: [Article] = []
        let c2 = encoded.publisher
            .decode(type: Article.self, decoder: JSONDecoder())
            .sink(receiveCompletion: { _ in },
                  receiveValue: { decoded.append($0) })
        defer { c2.cancel() }
        XCTAssertEqual(decoded, [article])
    }
}
