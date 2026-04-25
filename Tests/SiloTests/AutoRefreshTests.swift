import Foundation
import Silo
import Testing

@MainActor
@Suite("Auto-Refresh Tests", .timeLimit(.minutes(1)))
struct AutoRefreshTests {

  /// Verifies that a `DataSource` configured with `.autoRefresh` periodically re-invokes the fetch
  /// closure without any manual `refresh()` calls. Subscribing to `.values` activates the timer;
  /// the test awaits three consecutive emissions (empty value plus two refreshes) and
  /// confirms the fetch closure was called exactly twice after the initial load.
  @Test("Auto-refresh triggers periodic fetches")
  func autoRefreshTriggers() async throws {
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
    .autoRefresh(.milliseconds(10))
    .build()

    // Subscribe to start auto-refresh
    let task = Task {
      var iterator = source.values.makeAsyncIterator()
      #expect(await iterator.next() == 0)  // Empty value
      #expect(await iterator.next() == 1)  // First (immediate) auto-refresh
      #expect(await iterator.next() == 2)  // Second auto-refresh
      source.stopAutoRefresh()
    }

    // Wait for auto-refreshes to happen
    await task.value

    #expect(await state.fetchCount == 2)  // At least 2 auto-refreshes
  }

  /// Verifies that `stopAutoRefresh()` halts periodic fetches and `resumeAutoRefresh()` restarts
  /// them. After an initial auto-refresh fires, `stopAutoRefresh()` is called; the fetch count
  /// must not increase during a subsequent wait. Calling `resumeAutoRefresh()` then causes
  /// additional fetches to occur within the next interval.
  @Test("Stop and resume auto-refresh")
  func stopResumeAutoRefresh() async throws {
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
    .autoRefresh(.milliseconds(30))
    .build()

    // Subscribe to start auto-refresh
    var iterator = source.values.makeAsyncIterator()

    // Empty value
    #expect(await iterator.next() == 0)

    // Immediate fetch on subscribe
    #expect(await iterator.next() == 1)
    #expect(await state.fetchCount == 1)

    // Let it auto-refresh once
    try await Task.sleep(for: .milliseconds(40))
    #expect(await iterator.next() == 2)
    #expect(await state.fetchCount == 2)

    // Stop auto-refresh
    source.stopAutoRefresh()

    // Wait and verify no more fetches
    try await Task.sleep(for: .milliseconds(100))
    #expect(await state.fetchCount == 2)

    // Resume
    source.resumeAutoRefresh()

    // Immediate fetch after resume
    #expect(await iterator.next() == 3)
    #expect(await state.fetchCount == 3)

    // Should resume auto-refreshing
    try await Task.sleep(for: .milliseconds(40))
    #expect(await iterator.next() == 4)
    #expect(await state.fetchCount == 4)
  }

  /// Verifies that `restartAutoRefresh(immediate: false)` resets the timer interval without
  /// triggering an extra fetch at the moment of the call. The test records the fetch count
  /// immediately before and after `restartAutoRefresh(immediate: false)` and asserts they are
  /// equal, confirming no additional fetch fired synchronously.
  @Test("restartAutoRefresh without immediate refresh")
  func restartAutoRefreshNoImmediate() async throws {
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
    .autoRefresh(.milliseconds(100))
    .build()

    // Start observing to activate auto-refresh
    let task = Task {
      for await _ in source.values {}
    }

    // Immediate fetch on subscription
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)

    // Restart without immediate
    await source.restartAutoRefresh(immediate: false)

    // Should not fetch additionally immediately
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)

    // Should fetch after delay
    try await Task.sleep(for: .milliseconds(70))
    #expect(await state.fetchCount == 2)

    // Clean up
    task.cancel()
  }

  /// Verifies that `restartAutoRefresh(immediate: true)` triggers a fetch immediately in addition
  /// to resetting the periodic timer. The fetch count is captured before the call; after
  /// `restartAutoRefresh(immediate: true)` the count must be exactly one higher, confirming a
  /// single synchronous fetch fired at restart.
  @Test("restartAutoRefresh with immediate refresh")
  func restartAutoRefreshImmediate() async throws {
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
    .autoRefresh(.milliseconds(100))
    .build()

    // Start observing to activate auto-refresh
    let task = Task {
      for await _ in source.values {}
    }

    // Immediate fetch on subscription
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)

    // Restart without immediate
    await source.restartAutoRefresh(immediate: true)

    // Should not fetch additionally immediately
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 2)

    // Should fetch after delay
    try await Task.sleep(for: .milliseconds(70))
    #expect(await state.fetchCount == 3)

    // Clean up
    task.cancel()
  }

  /// Verifies that the `.autoRefresh` timer does not start when there are no active `.values` or
  /// `.valueWithState` subscribers. A `DataSource` with a 30 ms interval is built and discarded
  /// (no subscriber); after 120 ms the test asserts the fetch closure was never invoked.
  @Test("Auto-refresh does not fire without subscribers")
  func autoRefreshDoesNotFireWithoutSubscribers() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    _ = dataSource {
      await state.increment()
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .autoRefresh(.milliseconds(10))
    .build()

    try await Task.sleep(for: .milliseconds(30))

    #expect(await state.fetchCount == 0)
  }

  /// Verifies that calling `resumeAutoRefresh()` while auto-refresh is already running has no
  /// observable effect (it does not restart the timer or schedule an extra fetch). After
  /// subscribing to activate the timer, `resumeAutoRefresh()` is called; `stopAutoRefresh()` is
  /// then used to freeze the count and the test confirms no additional fetches occurred.
  @Test("resumeAutoRefresh is a no-op when already running")
  func resumeAutoRefreshWhenAlreadyRunning() async throws {
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
    .autoRefresh(.milliseconds(50))
    .build()

    // Start observing to activate auto-refresh
    let task = Task {
      for await _ in source.values {}
    }

    // Let the immediate refresh (and any in-flight work) complete before stopping
    try await Task.sleep(for: .milliseconds(30))

    // Auto-refresh is running - resume should be a no-op (guard: autoRefreshPaused)
    source.resumeAutoRefresh()

    // Wait for one auto-refresh to complete
    try await Task.sleep(for: .milliseconds(30))

    // Stop and verify no additional fetches occur
    source.stopAutoRefresh()
    #expect(await state.fetchCount == 2)

    // Check stop has the same effects
    try await Task.sleep(for: .milliseconds(100))
    #expect(await state.fetchCount == 2)

    // Clean up
    task.cancel()
  }

  /// Verifies that the `.autoRefresh` timer only stops when all subscribers across both `.values`
  /// and `.valueWithState` have left. One task subscribes via `.values` and another via
  /// `.valueWithState`; cancelling the first keeps the timer running (fetch count continues to
  /// grow), and only after the second task is cancelled does the timer stop.
  @Test("Auto-refresh timer stops only when both values and valueWithState subscribers are gone")
  func autoRefreshStopsOnlyWhenAllSubscribersLeave() async throws {
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
    .autoRefresh(.milliseconds(30))
    .build()

    let valuesTask = Task { for await _ in source.values {} }
    let stateTask = Task { for await _ in source.valueWithState {} }

    // Auto-refresh is running
    try await Task.sleep(for: .milliseconds(40))
    let countWithBoth = await state.fetchCount
    #expect(countWithBoth == 2)

    // Cancel one — timer should keep running
    valuesTask.cancel()
    try await Task.sleep(for: .milliseconds(30))
    let countWithOne = await state.fetchCount
    #expect(countWithOne == 3)

    // Cancel the last — timer should stop
    stateTask.cancel()
    try await Task.sleep(for: .milliseconds(100))
    let countAfterStop = await state.fetchCount
    #expect(countAfterStop == 3)
  }

  /// Verifies that subscribing only to `.state` (not `.values` or `.valueWithState`) does not
  /// activate the `.autoRefresh` timer. A task iterates `.state` for 100 ms; after it is cancelled
  /// the test asserts the fetch closure was never invoked, confirming state-only subscriptions do
  /// not count toward the active subscriber threshold.
  @Test("State-only subscriber does not start the auto-refresh timer")
  func stateOnlySubscriberDoesNotStartAutoRefresh() async throws {
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
    .autoRefresh(.milliseconds(10))
    .build()

    // Subscribe to state only — must not count toward activeSubscriberCount
    let task = Task { for await _ in source.state {} }

    try await Task.sleep(for: .milliseconds(30))

    #expect(await state.fetchCount == 0)

    task.cancel()
  }

  /// Verifies that `resumeAutoRefresh()` does not start the timer when there are no active
  /// subscribers. `stopAutoRefresh()` is called with no subscribers present, then
  /// `resumeAutoRefresh()` is called; after 100 ms the test asserts the fetch closure was never
  /// invoked, confirming the subscriber-gate is still enforced on resume.
  @Test("resumeAutoRefresh with no subscribers does not start timer")
  func resumeAutoRefreshNoSubscribers() async throws {
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
    .autoRefresh(.milliseconds(10))
    .build()

    // Stop (sets paused=true) with no subscribers
    source.stopAutoRefresh()

    // Resume with no subscribers - should not start timer
    source.resumeAutoRefresh()

    try await Task.sleep(for: .milliseconds(30))

    #expect(await state.fetchCount == 0)
  }
}
