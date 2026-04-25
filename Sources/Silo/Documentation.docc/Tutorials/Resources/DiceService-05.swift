import Silo

@MainActor
class DiceService {
    let rolls: DataSource<[Int]>

    init(sides: AsyncStream<Int>, count: AsyncStream<Int>) {
        rolls = dataSource(
            sides.dependency(.lazy, clear: true),  // new dice type → clear immediately
            count.dependency(.lazy)  // new count → keep showing previous rolls
        ) { sides, count in
            (0..<count).map { _ in Int.random(in: 1...sides) }
        } onError: { _ in
            .keep
        }
        .build()
    }
}
