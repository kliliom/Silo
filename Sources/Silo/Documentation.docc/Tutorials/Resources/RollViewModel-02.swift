import Silo

@MainActor
final class RollViewModel {
    var rolls: DataSource<[Int]> { service.rolls }

    private let service: DiceService
    private let sidesContinuation: AsyncStream<Int>.Continuation
    private let countContinuation: AsyncStream<Int>.Continuation

    init() {
        let (sidesStream, sidesCont) = AsyncStream.makeStream(of: Int.self)
        let (countStream, countCont) = AsyncStream.makeStream(of: Int.self)
        sidesContinuation = sidesCont
        countContinuation = countCont
        service = DiceService(sides: sidesStream, count: countStream)
        sidesCont.yield(6)
        countCont.yield(1)
    }

    func selectSides(_ sides: Int) {
        sidesContinuation.yield(sides)
    }

    func selectCount(_ count: Int) {
        countContinuation.yield(count)
    }

    func reroll() async throws {
        try await service.rolls.refresh(clear: true)
    }
}
