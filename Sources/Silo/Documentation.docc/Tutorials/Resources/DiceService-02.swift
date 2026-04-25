import Silo

@MainActor
class DiceService {
    let rolls: DataSource<[Int]>

    init(sides: AsyncStream<Int>) {
        // Re-roll whenever the number of sides changes
        rolls = dataSource(sides.dependency(.eager)) { sides in
            [Int.random(in: 1...sides)]
        } onError: { _ in
            .keep
        }
        .build()
    }
}
