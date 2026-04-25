import Silo

@MainActor
class DiceRoller {
    static let shared = DiceRoller()

    let lastRoll: DataSource<Int?>

    private init() {
        lastRoll = dataSource {
            try? await Task.sleep(for: .seconds(1))
            return Int.random(in: 1...6)
        } onError: { _ in
            .keep
        }
        .build()
    }
}
