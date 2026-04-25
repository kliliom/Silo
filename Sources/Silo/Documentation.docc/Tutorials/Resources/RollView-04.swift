import Silo
import SwiftUI

struct RollView: View {
    @State private var viewModel = RollViewModel()

    @State private var rolls: [Int] = []
    @State private var isRolling = false
    @State private var selectedSides = 6
    @State private var selectedCount = 1

    let sideOptions = [4, 6, 8, 10, 12, 20, 100]
    let countOptions = Array(1...6)

    var body: some View {
        List {
            Section("Configuration") {
                Picker("Sides", selection: $selectedSides) {
                    ForEach(sideOptions, id: \.self) { Text("D\($0)") }
                }
                Picker("Count", selection: $selectedCount) {
                    ForEach(countOptions, id: \.self) { Text("\($0)") }
                }
            }
            Section {
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
        .onChange(of: selectedSides) { _, newValue in
            viewModel.selectSides(newValue)
        }
        .onChange(of: selectedCount) { _, newValue in
            viewModel.selectCount(newValue)
        }
        .task {
            for await snapshot in viewModel.rolls.valueWithState {
                rolls = snapshot.value
                isRolling = snapshot.state.isRefreshing
            }
        }
    }
}
