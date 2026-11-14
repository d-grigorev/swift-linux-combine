import Foundation

/// One benchmark scenario: a self-contained workload returning its output count.
struct BenchScenario {
    let name: String
    let elements: Int
    let body: () -> Int
}

/// One measured scenario result.
struct ScenarioResult {
    let name: String
    let elements: Int
    let nanoseconds: UInt64
    /// Workload output (value count) — cross-checked between libraries.
    let count: Int
}

func measureMin(_ scenario: BenchScenario, iterations: Int) -> ScenarioResult {
    var best = UInt64.max
    var count = 0
    for _ in 0..<iterations {
        let start = DispatchTime.now().uptimeNanoseconds
        count = scenario.body()
        let elapsed = DispatchTime.now().uptimeNanoseconds &- start
        if elapsed < best { best = elapsed }
    }
    return ScenarioResult(
        name: scenario.name,
        elements: scenario.elements,
        nanoseconds: best,
        count: count
    )
}

@main
struct CombineBench {
    static func main() {
        let iterations = 5

        print("swift-combine performance challenge")
        print("baseline: OpenCombine @ 1c6f02c (same commit as the parity suite)")
        print("iterations per scenario: \(iterations) (min reported, order interleaved)")
        print("")

        let ours = Ours.scenarios()
        let theirsByName = Dictionary(
            uniqueKeysWithValues: Theirs.scenarios().map { ($0.name, $0) }
        )

        // Interleave: measure ours and the baseline back-to-back per scenario
        // to cancel allocator/CPU warm-up order bias.
        var oursResults: [ScenarioResult] = []
        var theirsResults: [ScenarioResult] = []
        for scenario in ours {
            oursResults.append(measureMin(scenario, iterations: iterations))
            if let base = theirsByName[scenario.name] {
                theirsResults.append(measureMin(base, iterations: iterations))
            }
        }

        let nameWidth = 28
        func header(_ label: String, _ width: Int) -> String {
            String(repeating: " ", count: max(0, width - label.count)) + label
        }
        print(
            header("scenario", nameWidth)
            + header("ours (ms)", 14)
            + header("opencomb (ms)", 15)
            + header("ratio", 9)
        )
        print(String(repeating: "-", count: nameWidth + 38))

        var mismatches = 0
        for result in oursResults {
            let mine = Double(result.nanoseconds) / 1_000_000
            if let base = theirsResults.first(where: { $0.name == result.name }) {
                let theirsMs = Double(base.nanoseconds) / 1_000_000
                let ratio = theirsMs > 0 ? mine / theirsMs : 1
                if base.count != result.count {
                    mismatches += 1
                    print("  COUNT MISMATCH in \(result.name): ours=\(result.count) theirs=\(base.count)")
                }
                print(
                    (result.name.padding(toLength: nameWidth, withPad: " ", startingAt: 0))
                    + String(format: "%14.2f %14.2f %7.2fx", mine, theirsMs, ratio)
                )
            } else {
                print(
                    (result.name.padding(toLength: nameWidth, withPad: " ", startingAt: 0))
                    + String(format: "%14.2f %14s %8s", mine, "n/a", "-")
                )
            }
        }
        print("")
        print(mismatches == 0
              ? "workload outputs match across libraries (correctness spot-check passed)"
              : "WARNING: \(mismatches) workload output mismatches")
    }
}
