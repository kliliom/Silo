import Silo

@MainActor
class DiceService {
    let rolls: DataSource<[Int]>

    init() {
        rolls = dataSource {
            [Int.random(in: 1...6)]
        } onError: { _ in
            .keep
        }
        .build()
    }
}
