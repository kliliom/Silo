import Silo

@MainActor
class DiceService {
    let rolls: DataSource<[Int]>

    init(sides: AsyncStream<Int>) {
        // clear: true: empties results immediately on dice type change
        rolls = dataSource(sides.dependency(.lazy, clear: true)) { sides in
            [Int.random(in: 1...sides)]
        } onError: { _ in
            .keep
        }
        .build()
    }
}
