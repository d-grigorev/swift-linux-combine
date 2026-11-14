import XCTest
import LinuxCombine
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking   // URLSession lives here on Linux
#endif

/// Executable counterparts of every `<!-- example: foundation/… -->`
/// snippet in docs/FOUNDATION.md (and the Russian edition).
final class FoundationExamplesTests: XCTestCase {

    func testFoundationNotificationcenter() {
        let center = NotificationCenter.default
        let name = Notification.Name("DocsExampleNotification")

        var titles: [String] = []
        let cancellable = center
            .publisher(for: name)
            .compactMap { $0.userInfo?["title"] as? String }
            .sink { titles.append($0) }
        defer { cancellable.cancel() }

        center.post(name: name, object: nil, userInfo: ["title": "First"])
        center.post(name: name, object: nil, userInfo: ["title": "Second"])

        XCTAssertEqual(titles, ["First", "Second"])
    }

    func testFoundationTimer() {
        let ticking = Timer.publish(every: 0.05, on: .main, in: .default)
            .autoconnect()

        var ticks = 0
        let cancellable = ticking.sink { _ in ticks += 1 }

        // Give the run loop time for at least two ticks, then stop.
        let deadline = Date().addingTimeInterval(0.3)
        RunLoop.current.run(until: deadline)
        cancellable.cancel()

        XCTAssertGreaterThanOrEqual(ticks, 2)
    }

    func testFoundationUrlsession() {
        // Construct only — documentation tests must not need network.
        let publisher = URLSession.shared.dataTaskPublisher(
            for: URL(string: "https://example.invalid/resource")!
        )
        XCTAssertNotNil(publisher)
    }
}
