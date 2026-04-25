import Foundation
import Silo
import Testing

@MainActor
@Suite("Debounce Tests", .timeLimit(.minutes(1)))
struct DebounceTests {

  /// Verifies that when a second `refresh()` supersedes a first one still waiting in the `.debounce()`
  /// window, the first caller receives a `CancellationError`. The first call enters the debounce
  /// wait, the second call resets the timer (cancelling the first), and the test asserts that the
  /// first caller's catch block sees `CancellationError`.
  @Test("Superseded debounce callers throw CancellationError")
  func debounceSuperseededCallsThrowCancellationError() async throws {
    actor State {
      var threwCancellation = false
      func setThrew() { threwCancellation = true }
    }
    let state = State()

    let source = dataSource {
      42
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .debounce(.milliseconds(100))
    .build()

    // First call — will be superseded by the second
    Task {
      do {
        _ = try await source.refresh()
      } catch is CancellationError {
        await state.setThrew()
      }
    }

    // Brief pause so the first call enters its debounce wait
    try await Task.sleep(for: .milliseconds(10))

    // Second call — increments the counter and cancels the first debounce task,
    // causing the first caller to throw CancellationError
    _ = try await source.refresh()

    try await Task.sleep(for: .milliseconds(30))
    #expect(await state.threwCancellation)
  }

  /// Verifies that `.debounce()` collapses rapid successive `refresh()` calls into a single fetch
  /// and that the surviving (un-cancelled) caller receives the fetched value. Three rapid calls
  /// are fired; the first two are expected to be superseded and the last one must both trigger
  /// the single fetch and return its result.
  @Test("Debounce delays execution and last caller receives the fetched value")
  func debounceDelaysExecution() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .debounce(.milliseconds(50))
    .build()

    // First two calls should be superseded; the last one wins and receives the value.
    let t1 = Task { try? await source.refresh() }
    let t2 = Task { try? await source.refresh() }
    try await Task.sleep(for: .milliseconds(5))
    let t3 = Task { try? await source.refresh() }

    _ = await t1.value  // superseded — returns nil via try?
    _ = await t2.value
    let last = await t3.value

    #expect(await state.fetchCount == 1)
    #expect(last == 1)
  }

  /// Verifies that each new `refresh()` call resets the debounce window rather than allowing the
  /// previous timer to fire. Calls are made every 30 ms within a 100 ms debounce window; as long as
  /// the gap stays under the window, the timer must continue to reset and no fetch must run.
  /// After the final call, the test waits for the window to close and asserts exactly one fetch ran.
  @Test("Debounce window resets on each call")
  func debounceWindowResets() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .debounce(.milliseconds(100))
    .build()

    // Five calls spaced 30 ms apart — each call resets the 100 ms window.
    for _ in 0..<5 {
      Task { _ = try? await source.refresh() }
      try await Task.sleep(for: .milliseconds(30))
    }

    // No fetch yet: the last call still has ~100 ms to wait.
    #expect(await state.fetchCount == 0)

    // Wait for the final debounce window to close.
    try await Task.sleep(for: .milliseconds(150))

    #expect(await state.fetchCount == 1)
  }

  /// Verifies that `clear()` cancels a pending `.debounce()` wait, causing the caller of `refresh()`
  /// that is sitting in the debounce window to receive a `CancellationError`. A refresh is started
  /// and left waiting in a 200 ms debounce window; `clear()` cancels the debounce task and the
  /// test asserts both that the error was thrown and that the fetch closure was never invoked.
  @Test("clear() cancels a pending debounce, causing the awaiting caller to throw CancellationError")
  func clearCancelsPendingDebounce() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .debounce(.milliseconds(200))
    .build()

    let task = Task { () -> Bool in
      do {
        _ = try await source.refresh()
        return false
      } catch is CancellationError {
        return true
      } catch {
        return false
      }
    }

    // Let the debounce timer start
    try await Task.sleep(for: .milliseconds(20))

    source.clear()

    #expect(await task.value)
    #expect(await state.fetchCount == 0)
  }
}
