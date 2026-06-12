import Foundation
import Silo
import Testing

@MainActor
@Suite("Builder Tests", .timeLimit(.minutes(1)))
struct BuilderTests {

  /// Verifies that calling `build()` a second time on the same builder traps with a
  /// precondition failure. Both data sources would share a single dependency coordinator,
  /// and dependency `AsyncStream`s only support a single consumer — a second build would
  /// silently split dependency emissions between the two sources.
  @Test("build() may only be called once per builder")
  func buildTwiceTraps() async throws {
    await #expect(processExitsWith: .failure) {
      await MainActor.run {
        let builder = dataSource {
          "data"
        } onError: { _ in
          .keep
        } emptyValue: {
          "empty"
        }
        _ = builder.build()
        _ = builder.build()
      }
    }
  }
}
