import Silo
import SwiftUI

struct DiceView: View {
    let roller = DiceRoller.shared

    @State private var roll: Int? = nil
    @State private var isRolling = false

    var body: some View {
        VStack(spacing: 24) {
            Text(roll.map(String.init) ?? "—")
                .font(.system(size: 80, weight: .bold, design: .rounded))
                .overlay {
                    if isRolling {
                        ProgressView()
                            .controlSize(.extraLarge)
                            .padding(24)
                            .background(.regularMaterial, in: .circle)
                    }
                }
        }
        .task {
            for await snapshot in roller.lastRoll.valueWithState {
                roll = snapshot.value
                isRolling = snapshot.state.isRefreshing
            }
        }
    }
}
