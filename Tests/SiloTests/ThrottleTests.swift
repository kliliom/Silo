import Foundation
import Silo
import Testing

@MainActor
@Suite("Throttle Tests", .timeLimit(.minutes(1)))
struct ThrottleTests {

  /// Verifies that `.throttle()` suppresses `refresh()` calls that arrive within the throttle
  /// window. The first call executes immediately; a second call within the 100 ms window returns
  /// the cached value without invoking the fetch closure; a third call after the window expires
  /// triggers a new fetch.
  @Test("Throttle limits refresh frequency")
  func throttleLimitsFrequency() async throws {
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
    .throttle(.milliseconds(100))
    .build()

    // First call executes immediately
    let first = try await source.refresh()
    #expect(first == 1)

    // Second call is ignored (throttled)
    let second = try await source.refresh()
    #expect(second == 1)  // Returns cached
    #expect(await state.fetchCount == 1)  // No new fetch

    // Wait for throttle window to end
    try await Task.sleep(for: .milliseconds(150))

    // Third call executes
    let third = try await source.refresh()
    #expect(third == 2)
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that `.throttle(_:last: true)` queues the most recent suppressed `refresh()` call
  /// and executes it once the throttle window expires. Three rapid calls are fired during the
  /// throttle window; after the window closes exactly one additional fetch runs, bringing the total
  /// to two.
  @Test("Throttle with last true queues last request")
  func throttleQueueLast() async throws {
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
    .throttle(.milliseconds(100), last: true)
    .build()

    // First request - should execute immediately
    _ = try await source.refresh()
    #expect(await state.fetchCount == 1)

    // Rapid requests during throttle window
    Task { _ = try? await source.refresh() }
    Task { _ = try? await source.refresh() }
    Task { _ = try? await source.refresh() }

    // Wait for throttle to expire and last request to execute
    try await Task.sleep(for: .milliseconds(150))

    // Should have executed first request + one queued request
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that with the default `last: false`, suppressed `refresh()` calls return the cached
  /// value immediately without queuing. Five rapid calls are fired during the throttle window;
  /// all of them return the first fetched value and no additional fetch runs after the window
  /// expires, confirming the "drop" semantics.
  @Test("Throttle with last: false drops in-window requests without queueing")
  func throttleDropsInWindowRequests() async throws {
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
    .throttle(.milliseconds(100))
    .build()

    let first = try await source.refresh()
    #expect(first == 1)

    // Five in-window calls — all return cached; none queued.
    for _ in 0..<5 {
      let result = try await source.refresh()
      #expect(result == 1)
    }
    #expect(await state.fetchCount == 1)

    // After the window expires, no queued fetch should fire spontaneously.
    try await Task.sleep(for: .milliseconds(150))
    #expect(await state.fetchCount == 1)
  }

  /// Verifies that the throttle window only starts when a fetch actually runs. A `refresh()`
  /// blocked by a failing prerequisite performs no fetch, so it must not open a throttle window
  /// that would suppress the next (now permitted) refresh. Historically the window was set in
  /// `refresh()` before the prerequisite check.
  @Test("Throttle window only starts when a fetch actually runs")
  func throttleWindowOnlyOnRealFetch() async throws {
    actor State {
      var allow = false
      var fetchCount = 0
      func setAllow(_ v: Bool) { allow = v }
      func increment() { fetchCount += 1 }
    }
    let state = State()

    struct Gate: DataSourceRefreshPrerequisite {
      let state: State
      func check() async -> Bool { await state.allow }
    }

    let source = dataSource {
      await state.increment()
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .throttle(.seconds(60))
    .requires(Gate(state: state))
    .build()

    // Prerequisite fails — no fetch happens, so no throttle window may start
    _ = try? await source.refresh()
    #expect(await state.fetchCount == 0)

    // The next refresh must not be throttled by the failed attempt
    await state.setAllow(true)
    let result = try await source.refresh()
    #expect(result == 1)
  }

  /// Verifies that a `refresh()` joining an already in-flight fetch (deduplication) does not
  /// advance the throttle window. The first fetch's window is allowed to expire while the fetch
  /// is still running; a second caller then joins the in-flight task, and a third refresh after
  /// completion must trigger a real fetch instead of being throttled by the joiner.
  @Test("Deduplicated refresh does not advance the throttle window")
  func dedupedRefreshDoesNotAdvanceThrottle() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()
    let fetchStarted = Semaphore(value: 0)
    let releaseFetch = Semaphore(value: 0)

    let source = dataSource {
      await state.increment()
      if await state.fetchCount == 1 {
        await fetchStarted.signal()
        await releaseFetch.wait()
      }
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .throttle(.milliseconds(100))
    .build()

    // First fetch: opens a 100 ms window, then blocks
    let first = Task { try await source.refresh() }
    await fetchStarted.wait()

    // Let the window expire while the fetch is still in flight
    try await Task.sleep(for: .milliseconds(150))

    // Joins the in-flight fetch — must not open a new window
    let second = Task { try await source.refresh() }

    await releaseFetch.signal()
    #expect(try await first.value == 1)
    #expect(try await second.value == 1)

    // No active window remains, so this must fetch for real
    let third = try await source.refresh()
    #expect(third == 2)
  }

  /// Verifies that `refresh(clear: true)` bypasses the throttle window, forcing a fetch even when
  /// the throttle would normally suppress the call. After a throttled fetch starts the window, a
  /// normal `refresh()` returns the cached value; a subsequent `refresh(clear: true)` invokes the
  /// fetch closure again, bringing the total to 2.
  @Test("refresh(clear: true) bypasses an active throttle window")
  func refreshClearBypassesThrottle() async throws {
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
    .throttle(.seconds(60))  // long window — only bypass should clear it
    .build()

    _ = try await source.refresh()
    #expect(await state.fetchCount == 1)

    // Normal refresh is throttled.
    let throttled = try await source.refresh()
    #expect(throttled == 1)
    #expect(await state.fetchCount == 1)

    // refresh(clear: true) clears state and bypasses the throttle.
    let fresh = try await source.refresh(clear: true)
    #expect(fresh == 2)
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that `clear()` resets the throttle expiry so the next `refresh()` triggers a real
  /// fetch even if the original throttle window (here 60 seconds) has not yet elapsed. After one
  /// fetch starts the throttle, `clear()` is called and the subsequent `refresh()` must invoke the
  /// fetch closure again, bringing the total count to 2.
  @Test("clear() resets the throttle window so the next refresh proceeds immediately")
  func clearResetsThrottle() async throws {
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
    .throttle(.seconds(60))
    .build()

    _ = try await source.refresh()
    #expect(await state.fetchCount == 1)

    source.clear()

    _ = try await source.refresh()
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that a throttled drop cannot masquerade the empty value as fetched data. The first
  /// fetch fails (opening the throttle window) and leaves the source empty; a `refresh()` inside
  /// the window must perform a real fetch instead of silently returning `emptyValue`, because
  /// with nothing cached there is no meaningful value to drop to.
  @Test("Throttled drop does not return the empty value after a failed first fetch")
  func throttleDropDoesNotMaskEmptyCache() async throws {
    actor State {
      var fetchCount = 0
      func increment() -> Int {
        fetchCount += 1
        return fetchCount
      }
    }
    let state = State()
    struct FetchError: Error {}

    let source = dataSource {
      let attempt = await state.increment()
      if attempt == 1 { throw FetchError() }
      return "data-\(attempt)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .throttle(.seconds(60))
    .build()

    // First fetch fails — a throttle window opened, but the source is still empty
    await #expect(throws: FetchError.self) {
      try await source.refresh()
    }

    // While empty, the in-window refresh fetches for real instead of returning "empty"
    let result = try await source.refresh()
    #expect(result == "data-2")
    #expect(await state.fetchCount == 2)
  }
}
