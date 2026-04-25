import Foundation
import Silo
import Testing

@MainActor
@Suite("Clearing Tests", .timeLimit(.minutes(1)))
struct ClearingTests {

  /// Verifies that `clear()` resets the `DataSource` cache to `emptyValue` and emits that value
  /// on the `.values` stream. After a successful `refresh()` populates the cache with `"value"`,
  /// `clear()` is called and the next emission from `.values` must equal `"empty"`.
  @Test("Manual clearing")
  func manualClearing() async throws {
    let source = dataSource {
      "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == "value")

    // Manually clear
    source.clear()

    let second = await iterator.next()
    #expect(second == "empty")
  }

  /// Verifies that `cancelRefresh()` stops an in-flight fetch task before it can complete. A fetch
  /// closure that sleeps for 10 seconds is started; `cancelRefresh()` is called once the fetch has
  /// signalled it started, and the test asserts that the code after `Task.sleep` (which sets
  /// `completed = true`) was never reached.
  @Test("cancelRefresh stops ongoing fetch")
  func cancelRefreshStopsFetch() async throws {
    actor State {
      var completed = false
      func setCompleted() { completed = true }
    }
    let state = State()
    let fetchStarted = Semaphore(value: 0)

    let source = dataSource {
      await fetchStarted.signal()
      try await Task.sleep(for: .seconds(10))
      await state.setCompleted()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Start refresh in background
    Task {
      _ = try? await source.refresh()
    }

    // Wait until the fetch has actually started before cancelling
    await fetchStarted.wait()
    source.cancelRefresh()

    // setCompleted() is after Task.sleep which throws on cancellation — it cannot run
    #expect(await state.completed == false)
  }

  /// Verifies that `cancelRefresh(clear: true)` resets the cache to `emptyValue` even when no
  /// fetch is currently in flight. After a successful `refresh()` populates the cache, calling
  /// `cancelRefresh(clear: true)` causes the next `.values` emission to equal `"empty"`.
  @Test("cancelRefresh with clear clears data")
  func cancelRefreshWithClear() async throws {
    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Fetch some data
    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let cached = await iterator.next()
    #expect(cached == "data")

    // Cancel with clear
    source.cancelRefresh(clear: true)

    let cleared = await iterator.next()
    #expect(cleared == "empty")
  }

  /// Verifies that a `DataSource` remains usable after `cancelRefresh()` interrupts an in-flight
  /// fetch. A slow fetch is cancelled mid-flight; a subsequent `refresh()` on the *same* source
  /// must run to completion without being blocked by stale in-flight state.
  @Test("cancelRefresh then refresh works correctly")
  func cancelRefreshThenRefresh() async throws {
    actor State {
      var shouldSleep = true
      var fetchCount = 0
      func setShouldSleep(_ v: Bool) { shouldSleep = v }
      func increment() { fetchCount += 1 }
    }
    let state = State()
    let fetchStarted = Semaphore(value: 0)

    let source = dataSource {
      await state.increment()
      if await state.shouldSleep {
        await fetchStarted.signal()
        try await Task.sleep(for: .seconds(10))
      }
      return "data-\(await state.fetchCount)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    Task { _ = try? await source.refresh() }
    await fetchStarted.wait()

    source.cancelRefresh()

    // Same source should be usable — the next refresh runs the fetch to completion
    await state.setShouldSleep(false)
    let result = try await source.refresh()
    #expect(result == "data-2")
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that calling `clear()` while a fetch is in progress immediately emits `emptyValue`
  /// to the `.values` stream without waiting for the fetch to finish. A slow fetch is started and a
  /// semaphore is used to confirm it has begun; `clear()` is then called and the next `.values`
  /// emission must equal `"empty"` before the fetch completes.
  @Test("clear() during in-flight fetch emits empty value immediately")
  func clearDuringInFlightFetch() async throws {
    actor State {
      var shouldSleep = false
      func setShouldSleep(_ v: Bool) { shouldSleep = v }
    }
    let state = State()
    let fetchStarted = Semaphore(value: 0)

    let source = dataSource {
      if await state.shouldSleep {
        await fetchStarted.signal()
        try await Task.sleep(for: .seconds(10))
      }
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "data")

    await state.setShouldSleep(true)
    Task { _ = try? await source.refresh() }
    await fetchStarted.wait()

    source.clear()
    let afterClear = await iterator.next()
    #expect(afterClear == "empty")
  }

  /// Verifies that `cancelRefresh(clear: true)` both cancels an in-flight fetch and resets the
  /// cache to `emptyValue` in a single call. A slow fetch is started; once confirmed in-flight,
  /// `cancelRefresh(clear: true)` is called and the test asserts the next `.values` emission is
  /// `"empty"`.
  @Test("cancelRefresh(clear: true) during in-flight fetch cancels and clears")
  func cancelRefreshClearDuringFetch() async throws {
    actor State {
      var shouldSleep = false
      func setShouldSleep(_ v: Bool) { shouldSleep = v }
    }
    let state = State()
    let fetchStarted = Semaphore(value: 0)

    let source = dataSource {
      if await state.shouldSleep {
        await fetchStarted.signal()
        try await Task.sleep(for: .seconds(10))
      }
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "data")

    await state.setShouldSleep(true)
    Task { _ = try? await source.refresh() }
    await fetchStarted.wait()

    source.cancelRefresh(clear: true)

    let cleared = await iterator.next()
    #expect(cleared == "empty")
  }

  /// Verifies that `clear()` emits a `DataSourceState` with `isEmpty == true` to the `.state`
  /// stream after a prior fetch had set `isEmpty == false`. The test reads a post-fetch state
  /// snapshot confirming `isEmpty == false`, then calls `clear()` and asserts the next state
  /// snapshot has both `isEmpty == true` and `isRefreshing == false`.
  @Test("clear() emits isEmpty: true to the state stream")
  func clearEmitsToStateStream() async throws {
    let source = dataSource {
      "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var stateIterator = source.state.makeAsyncIterator()
    let beforeClear = await stateIterator.next()
    #expect(beforeClear?.isEmpty == false)

    source.clear()

    let afterClear = await stateIterator.next()
    #expect(afterClear?.isEmpty == true)
    #expect(afterClear?.isRefreshing == false)
  }

  /// Verifies that `cancelRefresh()` propagates a `CancellationError` to all callers currently
  /// awaiting a `refresh()`. A slow in-flight fetch is started; `cancelRefresh()` is called once
  /// the fetch begins, and the test asserts the caller's catch block receives `CancellationError`.
  @Test("cancelRefresh() causes awaiting refresh() callers to receive CancellationError")
  func cancelRefreshThrowsCancellationErrorToCaller() async throws {
    let fetchStarted = Semaphore(value: 0)

    let source = dataSource {
      await fetchStarted.signal()
      try await Task.sleep(for: .seconds(10))
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
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

    await fetchStarted.wait()
    source.cancelRefresh()

    #expect(await task.value)
  }

  /// Verifies that `refresh(clear: true)` emits `emptyValue` on the `.values` stream before the
  /// new fetch result arrives, giving subscribers a visible "loading" transition. After a prior
  /// fetch the test calls `refresh(clear: true)` and reads two consecutive values, asserting the
  /// order is `"empty"` followed by `"fetched"`.
  @Test("refresh(clear:true) emits empty value before fetching")
  func refreshWithClearEmitsEmptyFirst() async throws {
    let source = dataSource {
      "fetched"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "fetched")

    // refresh(clear: true) emits "empty" first, then "fetched" after the fetch
    try await source.refresh(clear: true)

    let afterClearEmpty = await iterator.next()
    let afterClearFetched = await iterator.next()
    #expect(afterClearEmpty == "empty")
    #expect(afterClearFetched == "fetched")
  }
}
