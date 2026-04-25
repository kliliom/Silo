import Silo

@MainActor
class DiceService {
    let rolls: DataSource<[Int]>

    init(sides: AsyncStream<Int>) {
        // .lazy: only re-rolls while the view is on screen
        rolls = dataSource(sides.dependency(.lazy)) { sides in
            [Int.random(in: 1...sides)]
        } onError: { _ in
            .keep
        }
        .build()
    }
}
