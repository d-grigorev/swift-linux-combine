import LinuxCombine
import Dispatch
import Foundation

/// Scenarios against **our** module (file-scoped `import Combine`, so every
/// bare Combine name resolves here).
enum Ours {

    static func scenarios() -> [BenchScenario] {
        [
            // 1 subject → 4 sinks: fan-out lock/dispatch cost.
            BenchScenario(name: "subjectFanOut(4x, 200k)", elements: 200_000) {
                let subject = PassthroughSubject<Int, Never>()
                var total = 0
                var bags: [AnyCancellable] = []
                for _ in 0..<4 {
                    bags.append(subject.sink { _ in total += 1 })
                }
                for i in 0..<200_000 { subject.send(i) }
                subject.send(completion: .finished)
                withExtendedLifetime(bags) {}
                return total
            },

            // chain: subject → map → filter → sink: per-event operator cost.
            BenchScenario(name: "mapFilterChain(100k)", elements: 100_000) {
                let subject = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = subject
                    .map { $0 &* 2 }
                    .filter { $0 % 3 == 0 }
                    .sink { _ in total += 1 }
                for i in 0..<100_000 { subject.send(i) }
                subject.send(completion: .finished)
                withExtendedLifetime(bag) {}
                return total
            },

            // cold publisher: sequence iteration + operator.
            BenchScenario(name: "sequenceMap(200k)", elements: 200_000) {
                var total = 0
                var bag: AnyCancellable?
                bag = (0..<200_000).publisher
                    .map { $0 &+ 1 }
                    .sink { _ in total += 1 }
                withExtendedLifetime(bag) {}
                return total
            },

            // zip: pairing two streams with full upstream buffering.
            BenchScenario(name: "zipPairs(50k)", elements: 50_000) {
                let a = PassthroughSubject<Int, Never>()
                let b = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = a.zip(b).sink { _ in total += 1 }
                for i in 0..<50_000 { a.send(i) }
                for i in 0..<50_000 { b.send(i) }
                withExtendedLifetime(bag) {}
                return total
            },

            // child publisher allocation + inner subscription churn.
            BenchScenario(name: "flatMapJust(50k)", elements: 50_000) {
                let subject = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = subject.flatMap { Just($0) }.sink { _ in total += 1 }
                for i in 0..<50_000 { subject.send(i) }
                subject.send(completion: .finished)
                withExtendedLifetime(bag) {}
                return total
            },

            // type erasure indirection.
            BenchScenario(name: "erasedSink(100k)", elements: 100_000) {
                let subject = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = subject.map { $0 &+ 1 }.eraseToAnyPublisher().sink { _ in total += 1 }
                for i in 0..<100_000 { subject.send(i) }
                subject.send(completion: .finished)
                withExtendedLifetime(bag) {}
                return total
            },

            // AnyCancellable store/release churn.
            BenchScenario(name: "anyCancellableChurn(50k)", elements: 50_000) {
                var array: [AnyCancellable] = []
                array.reserveCapacity(50_000)
                for i in 0..<50_000 {
                    array.append(AnyCancellable { _ = i })
                }
                let n = array.count
                array.removeAll(keepingCapacity: true)
                return n
            },

            // (ours only — no OpenCombine oracle) combineLatest interleaving.
            BenchScenario(name: "combineLatestBurst(25k x2)", elements: 50_000) {
                let a = PassthroughSubject<Int, Never>()
                let b = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = a.combineLatest(b).sink { _ in total += 1 }
                for i in 0..<25_000 {
                    a.send(i)
                    b.send(i)
                }
                withExtendedLifetime(bag) {}
                return total
            },

            // (ours only — no OpenCombine oracle) merge interleaving.
            BenchScenario(name: "mergeInterleave(2x50k)", elements: 100_000) {
                let a = PassthroughSubject<Int, Never>()
                let b = PassthroughSubject<Int, Never>()
                var total = 0
                var bag: AnyCancellable?
                bag = a.merge(with: b).sink { _ in total += 1 }
                for i in 0..<50_000 { a.send(i) }
                for i in 0..<50_000 { b.send(i) }
                withExtendedLifetime(bag) {}
                return total
            },
        ]
    }
}
