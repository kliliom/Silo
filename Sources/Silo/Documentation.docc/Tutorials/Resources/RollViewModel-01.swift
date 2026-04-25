import Silo

@MainActor
final class RollViewModel {
    let service: DiceService

    private let sidesContinuation: AsyncStream<Int>.Continuation
    private let countContinuation: AsyncStream<Int>.Continuation

    init() {
        let (sidesStream, sidesCont) = AsyncStream.makeStream(of: Int.self)
        let (countStream, countCont) = AsyncStream.makeStream(of: Int.self)
        sidesContinuation = sidesCont
        countContinuation = countCont
        service = DiceService(sides: sidesStream, count: countStream)
        // Seed initial values so both dependencies emit before the view subscribes
        sidesCont.yield(6)
        countCont.yield(1)
    }
}
