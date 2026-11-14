import LinuxCombine

/// A subscriber that records everything it receives and lets the test
/// drive demand explicitly (used by the demand/backpressure examples).
final class ManualDemandSubscriber<Input, Failure: Error>: Subscriber {
    private(set) var values: [Input] = []
    private(set) var completions: [Subscribers.Completion<Failure>] = []
    private(set) var subscription: (any Subscription)?

    func receive(subscription: any Subscription) {
        self.subscription = subscription
    }

    func receive(_ input: Input) -> Subscribers.Demand {
        values.append(input)
        return .none
    }

    func receive(completion: Subscribers.Completion<Failure>) {
        completions.append(completion)
    }

    func request(_ demand: Subscribers.Demand) {
        subscription?.request(demand)
    }
}
