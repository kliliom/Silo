import Silo
import SwiftUI

struct RollView: View {
    @State private var viewModel = RollViewModel()

    @State private var rolls: [Int] = []
    @State private var isRolling = false

    var body: some View {
        List {
            ForEach(rolls.indices, id: \.self) { i in
                Text("Die \(i + 1): \(rolls[i])")
                    .font(.title2)
            }
            if rolls.count > 1 {
                Text("Total: \(rolls.reduce(0, +))")
                    .font(.headline)
                    .foregroundStyle(.secondary)
            }
        }
        .overlay {
            if isRolling && rolls.isEmpty {
                ProgressView("Rolling…")
            }
        }
        .safeAreaInset(edge: .bottom) {
            Button("Re-roll") {
                Task { try? await viewModel.reroll() }
            }
            .buttonStyle(.borderedProminent)
            .padding()
        }
        .task {
            for await snapshot in viewModel.rolls.valueWithState {
                rolls = snapshot.value
                isRolling = snapshot.state.isRefreshing
            }
        }
    }
}
