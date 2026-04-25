import Foundation
import Silo
import Testing

@MainActor
@Suite("Prerequisites Tests", .timeLimit(.minutes(1)))
struct PrerequisitesTests {

  /// Verifies that a `DataSourceRefreshPrerequisite` whose `check()` returns `true` allows the
  /// fetch closure to run normally. Registers an always-passing prerequisite via `.requires()`,
  /// calls `refresh()`, and asserts the fetch closure was invoked once and returned the expected
  /// value.
  @Test("Prerequisites that pass allow fetch")
  func prerequisitesPass() async throws {
    struct AlwaysPass: DataSourceRefreshPrerequisite {
      func check() async -> Bool {
        true
      }
    }

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
    .requires(AlwaysPass())
    .build()

    let result = try await source.refresh()
    #expect(result == "data")
    #expect(await state.fetchCount == 1)
  }

  /// Verifies that a `DataSourceRefreshPrerequisite` whose `check()` returns `false` blocks the
  /// fetch closure entirely. Registers an always-failing prerequisite, calls `refresh()`, and
  /// asserts that a `PrerequisiteError` is thrown with the expected message and that the fetch
  /// closure was never invoked.
  @Test("Prerequisites that fail prevent fetch")
  func prerequisitesFail() async throws {
    struct AlwaysFail: DataSourceRefreshPrerequisite {
      func check() async -> Bool {
        false
      }
    }

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
    .requires(AlwaysFail())
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected prerequisite error")
    } catch let error as PrerequisiteError {
      #expect(error.message == "Prerequisite check failed")
      #expect(await state.fetchCount == 0)
    }
  }

  /// Verifies that prerequisite evaluation is short-circuit — once one `DataSourceRefreshPrerequisite`
  /// fails, subsequent prerequisites are not checked. Registers a failing prerequisite followed by a
  /// passing one and asserts that `check()` is called exactly once (only on the failing one).
  @Test("First failing prerequisite stops checking remaining ones")
  func firstFailingPrerequisiteStopsChecking() async throws {
    actor State {
      var checkCount = 0
      func increment() { checkCount += 1 }
    }
    let state = State()

    struct CountingPrerequisite: DataSourceRefreshPrerequisite {
      let state: State
      let passes: Bool
      func check() async -> Bool {
        await state.increment()
        return passes
      }
    }

    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .requires(CountingPrerequisite(state: state, passes: false))
    .requires(CountingPrerequisite(state: state, passes: true))
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected prerequisite error")
    } catch is PrerequisiteError {
      #expect(await state.checkCount == 1)
    }
  }

  /// Verifies that a `PrerequisiteError` is propagated directly to the caller and does not pass
  /// through the `onError` handler. Registers an always-failing prerequisite alongside an `onError`
  /// closure that increments a counter, then asserts the counter remains zero after `refresh()` throws.
  @Test("PrerequisiteError is not passed to onError handler")
  func prerequisiteErrorBypassesOnError() async throws {
    struct AlwaysFail: DataSourceRefreshPrerequisite {
      func check() async -> Bool { false }
    }

    actor State {
      var onErrorCallCount = 0
      func increment() { onErrorCallCount += 1 }
    }
    let state = State()

    let source = dataSource {
      "data"
    } onError: { _ in
      await state.increment()
      return .keep
    } emptyValue: {
      "empty"
    }
    .requires(AlwaysFail())
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected prerequisite error")
    } catch is PrerequisiteError {
      // Expected
    }

    #expect(await state.onErrorCallCount == 0)
  }

  /// Verifies that a `PrerequisiteError` bypasses the `.retry()` mechanism entirely. Even with
  /// `retry(count: 3)` configured, a failing prerequisite causes `refresh()` to throw immediately
  /// after a single `check()` call, with the fetch closure never invoked.
  @Test("PrerequisiteError does not trigger retry")
  func prerequisiteErrorDoesNotRetry() async throws {
    actor State {
      var checkCount = 0
      var fetchCount = 0
      func incrementCheck() { checkCount += 1 }
      func incrementFetch() { fetchCount += 1 }
    }
    let state = State()

    struct CountingFail: DataSourceRefreshPrerequisite {
      let state: State
      func check() async -> Bool {
        await state.incrementCheck()
        return false
      }
    }

    let source = dataSource {
      await state.incrementFetch()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .requires(CountingFail(state: state))
    .retry(count: 3)
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected prerequisite error")
    } catch is PrerequisiteError {
      // Expected
    }

    #expect(await state.checkCount == 1)
    #expect(await state.fetchCount == 0)
  }

  /// Verifies that all registered prerequisites must pass for a fetch to proceed. Registers a
  /// mix of passing and failing prerequisites via `.requires()`, calls `refresh()`, and asserts that
  /// a `PrerequisiteError` is thrown and the fetch closure is never invoked.
  @Test("Multiple prerequisites must all pass")
  func multiplePrerequisites() async throws {
    struct Pass: DataSourceRefreshPrerequisite {
      func check() async -> Bool { true }
    }

    struct Fail: DataSourceRefreshPrerequisite {
      func check() async -> Bool { false }
    }

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
    .requires(Pass())
    .requires(Fail())
    .requires(Pass())
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected prerequisite error")
    } catch is PrerequisiteError {
      #expect(await state.fetchCount == 0)
    }
  }
}
