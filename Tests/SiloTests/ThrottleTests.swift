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
}
