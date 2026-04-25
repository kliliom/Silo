import Foundation
import Silo
import Testing

@MainActor
@Suite("TTL Tests", .timeLimit(.minutes(1)))
struct TTLTests {

  /// Verifies that `.ttl()` prevents redundant network calls while the cached value is still fresh.
  /// Calls `refresh()` three times — once before the TTL expires (which must return the cached
  /// result without invoking the fetch closure) and once after the TTL elapses — asserting that the
  /// fetch closure is called exactly twice in total.
  @Test("TTL prevents fetch within duration")
  func ttlPreventsFetch() async throws {
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
    .ttl(.milliseconds(100))
    .build()

    // First fetch
    let first = try await source.refresh()
    #expect(first == 1)
    #expect(await state.fetchCount == 1)

    // Second fetch within TTL - should return cached
    let second = try await source.refresh()
    #expect(second == 1)
    #expect(await state.fetchCount == 1)  // No new fetch

    // Wait for TTL to expire
    try await Task.sleep(for: .milliseconds(150))

    // Third fetch after TTL - should fetch again
    let third = try await source.refresh()
    #expect(third == 2)
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that `.ttl(_:clear: true)` automatically resets the `DataSource` to its empty value
  /// once the TTL timer fires. After a successful `refresh()`, the test waits for the TTL to
  /// expire and asserts that `.values` emits the configured `emptyValue`.
  @Test("TTL with clear clears value on expiry")
  func ttlWithClear() async throws {
    let source = dataSource {
      "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .ttl(.milliseconds(50), clear: true)
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == "value")

    // Wait for TTL to expire
    try await Task.sleep(for: .milliseconds(100))

    // Should have been cleared
    let second = await iterator.next()
    #expect(second == "empty")
  }

  /// Verifies that `refresh(clear: true)` forces a new fetch even when the TTL has not expired.
  /// After a successful fetch populates the cache, a normal `refresh()` returns the cached value
  /// (fetch count unchanged), while `refresh(clear: true)` bypasses the TTL gate and invokes the
  /// fetch closure again.
  @Test("refresh(clear:true) bypasses active TTL")
  func refreshClearBypassesTTL() async throws {
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
    .ttl(.seconds(60))
    .build()

    let first = try await source.refresh()
    #expect(first == 1)
    #expect(await state.fetchCount == 1)

    // Within TTL - normal refresh returns cached
    let cached = try await source.refresh()
    #expect(cached == 1)
    #expect(await state.fetchCount == 1)

    // refresh(clear:true) bypasses TTL and fetches fresh
    let fresh = try await source.refresh(clear: true)
    #expect(fresh == 2)
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that calling `.ttl()` multiple times on a `DataSourceBuilder` uses only the last
  /// configured duration. The builder is chained with a 60-second TTL followed by a 50-millisecond
  /// TTL; the test confirms the short TTL takes effect by observing that the cache expires within
  /// ~100 ms, triggering a second fetch.
  @Test("Calling .ttl() twice uses the last duration")
  func ttlCalledTwiceUsesLast() async throws {
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
    .ttl(.seconds(60))  // long — should be overridden
    .ttl(.milliseconds(50))  // short — should win
    .build()

    let first = try await source.refresh()
    #expect(first == 1)

    // Within the short TTL — should use cache
    let cached = try await source.refresh()
    #expect(cached == 1)
    #expect(await state.fetchCount == 1)

    // Wait for the short TTL to expire
    try await Task.sleep(for: .milliseconds(100))

    // If the long TTL had won, this would still return 1
    let fresh = try await source.refresh()
    #expect(fresh == 2)
    #expect(await state.fetchCount == 2)
  }

  /// Verifies that `.beforeFetch()` hooks are skipped entirely when a TTL cache hit prevents the
  /// fetch closure from running. After the first successful `refresh()` (which runs the hook), a
  /// second `refresh()` within the 60-second TTL window must not invoke the hook again; the hook
  /// counter must remain at 1.
  @Test("beforeFetch hook does not run when TTL is fresh")
  func beforeFetchSkippedOnTTLHit() async throws {
    actor State {
      var hookCount = 0
      func increment() { hookCount += 1 }
    }
    let state = State()

    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .ttl(.seconds(60))
    .beforeFetch { await state.increment() }
    .build()

    try await source.refresh()  // hook runs, fetch runs
    try await source.refresh()  // TTL fresh — hook and fetch both skipped

    #expect(await state.hookCount == 1)
  }

  /// Verifies that calling `clear()` while a long TTL is active invalidates the cached state so
  /// that the next `refresh()` performs a real fetch instead of returning the stale cached value.
  /// After one successful fetch, `clear()` is called (making the source empty), and the subsequent
  /// `refresh()` must invoke the fetch closure again, returning an incremented count.
  @Test("clear() resets TTL so next refresh fetches fresh")
  func clearResetsTTLTimer() async throws {
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
    .ttl(.seconds(60))
    .build()

    try await source.refresh()
    #expect(await state.fetchCount == 1)

    // clear() resets TTL state (isEmpty becomes true)
    source.clear()

    // Next refresh should fetch fresh because isEmpty = true after clear
    let result = try await source.refresh()
    #expect(result == 2)
    #expect(await state.fetchCount == 2)
  }
}
