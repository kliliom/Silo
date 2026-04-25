import Silo

@MainActor
class DiceRoller {
    static let shared = DiceRoller()

    private init() {}
}
