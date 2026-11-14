import XCTest
import LinuxCombine

/// Executable counterparts of every `<!-- example: subjects/… -->`
/// snippet in docs/SUBJECTS-AND-STATE.md (and the Russian edition).
final class SubjectsExamplesTests: XCTestCase {

    func testSubjectsPassthrough() {
        let subject = PassthroughSubject<String, Never>()

        var seen: [String] = []
        let cancellable = subject.sink { seen.append($0) }
        defer { cancellable.cancel() }

        subject.send("early")
        subject.send(completion: .finished)
        subject.send("late")        // ignored — already finished

        XCTAssertEqual(seen, ["early"])
    }

    func testSubjectsCurrentvalue() {
        let name = CurrentValueSubject<String, Never>("initial")

        var seen: [String] = []
        let cancellable = name.sink { seen.append($0) }
        defer { cancellable.cancel() }
        XCTAssertEqual(seen, ["initial"], "the retained value first")

        name.send("next")
        XCTAssertEqual(seen, ["initial", "next"])

        XCTAssertEqual(name.value, "next")
        name.value = "assigning value == send"
        XCTAssertEqual(seen, ["initial", "next", "assigning value == send"])
    }

    func testSubjectsRecord() {
        let history = Record<Int, Never>(output: [1, 2, 3], completion: .finished)

        var first: [Int] = []
        let c1 = history.sink(
            receiveCompletion: { _ in },
            receiveValue: { first.append($0) }
        )
        defer { c1.cancel() }

        var second: [Int] = []
        let c2 = history.sink(
            receiveCompletion: { _ in },
            receiveValue: { second.append($0) }
        )
        defer { c2.cancel() }

        XCTAssertEqual(first, [1, 2, 3])
        XCTAssertEqual(second, [1, 2, 3])
    }

    func testSubjectsPublished() {
        final class Model {
            @Published var count = 0
        }
        let model = Model()

        var seen: [Int] = []
        let cancellable = model.$count.sink { seen.append($0) }
        defer { cancellable.cancel() }
        XCTAssertEqual(seen, [0], "the projected value publishes the current one")

        model.count = 1
        model.count = 2
        XCTAssertEqual(seen, [0, 1, 2])
    }

    func testSubjectsObservableObject() {
        final class Settings: ObservableObject {
            @Published var darkMode = false
            @Published var fontSize = 12
        }

        let settings = Settings()
        var changeCount = 0
        let cancellable = settings.objectWillChange.sink { _ in changeCount += 1 }
        defer { cancellable.cancel() }

        settings.darkMode = true
        settings.fontSize = 14
        XCTAssertEqual(changeCount, 2, "one notification per published mutation")
        XCTAssertTrue(settings.darkMode)
        XCTAssertEqual(settings.fontSize, 14)
    }
}
