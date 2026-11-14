import Dispatch
import Foundation
import OpenCombine

/// The same scenarios against **OpenCombine** (file-scoped
/// `import OpenCombine` — no bare-name ambiguity with our module).
///
/// NOTE: no combineLatest/merge scenarios — OpenCombine implements
/// neither (see docs/QUIRKS.md §B).
enum Theirs {

    static func scenarios() -> [BenchScenario] {
        [
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

            BenchScenario(name: "sequenceMap(200k)", elements: 200_000) {
                var total = 0
                var bag: AnyCancellable?
                bag = (0..<200_000).publisher
                    .map { $0 &+ 1 }
                    .sink { _ in total += 1 }
                withExtendedLifetime(bag) {}
                return total
            },

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
        ]
    }
}
