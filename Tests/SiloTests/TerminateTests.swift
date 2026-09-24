import Foundation
import Silo
import Testing

@MainActor
@Suite("Terminate Tests", .timeLimit(.minutes(1)))
struct TerminateTests {

  /// Verifies that `terminate()` finishes all active async streams — `.values`, `.state`, and
  /// `.valueWithState` — causing their iterators to return `nil`. Active iterators for all three
  /// streams are positioned past the initial value before `terminate()` is called; each subsequent
  /// `next()` call must return `nil`.
  @Test("terminate() finishes all active streams")
  func terminateFinishesAllStreams() async throws {
    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var valuesIterator = source.values.makeAsyncIterator()
    var stateIterator = source.state.makeAsyncIterator()
    var vwsIterator = source.valueWithState.makeAsyncIterator()

    _ = await valuesIterator.next()
    _ = await stateIterator.next()
    _ = await vwsIterator.next()

    source.terminate()

    #expect(await valuesIterator.next() == nil)
    #expect(await stateIterator.next() == nil)
    #expect(await vwsIterator.next() == nil)
  }

  /// Verifies that `terminate()` cancels any in-flight fetch task, preventing it from completing.
  /// A slow fetch that sleeps for 10 seconds signals when it starts; `terminate()` is called once
  /// the fetch is confirmed in-flight, and the test asserts the post-sleep side-effect (`completed
  /// = true`) was never reached.
  @Test("terminate() cancels an in-flight fetch")
  func terminateCancelsInFlightFetch() async throws {
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

    Task { _ = try? await source.refresh() }
    await fetchStarted.wait()

    source.terminate()

    // The fetch task was cancelled; the line after the sleep cannot run
    #expect(await state.completed == false)
  }

  /// Verifies that `terminate()` is idempotent — calling it multiple times in a row does not crash
  /// and leaves the source terminated: a `refresh()` afterwards throws `CancellationError` without
  /// fetching.
  @Test("terminate() is idempotent")
  func terminateIsIdempotent() async throws {
    let fetchCount = Counter()
    let source = dataSource {
      await fetchCount.increment()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    source.terminate()
    source.terminate()
    source.terminate()

    await #expect(throws: CancellationError.self) { try await source.refresh() }
    #expect(await fetchCount.value == 0)
  }

  /// Verifies that `terminate()` is final: streams created afterwards finish immediately without
  /// replaying a value, `reset()` cannot revive the source, and `refresh()` keeps throwing.
  @Test("terminate() is final: new streams finish, refresh throws, reset has no effect")
  func terminateIsFinal() async throws {
    let fetchCount = Counter()
    let source = dataSource {
      await fetchCount.increment()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .autoRefresh(.milliseconds(10))
    .build()

    _ = try await source.refresh()
    source.terminate()
    source.reset()

    var values = source.values.makeAsyncIterator()
    var state = source.state.makeAsyncIterator()
    var valueWithState = source.valueWithState.makeAsyncIterator()
    #expect(await values.next() == nil)
    #expect(await state.next() == nil)
    #expect(await valueWithState.next() == nil)

    await #expect(throws: CancellationError.self) { try await source.refresh() }
    await source.restartAutoRefresh(immediate: true)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await fetchCount.value == 1)
  }

  /// Verifies that a caller already inside `refresh()` when `terminate()` runs receives
  /// `CancellationError`.
  @Test("terminate() cancels callers waiting on an in-flight fetch")
  func terminateCancelsInFlightCaller() async throws {
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

    let caller = Task { try await source.refresh() }
    await fetchStarted.wait()
    source.terminate()

    await #expect(throws: CancellationError.self) { try await caller.value }
  }

  /// Verifies that `terminate()` stops dependency observation: a later emission does not fetch.
  @Test("terminate() stops dependency observation")
  func terminateStopsDependencies() async throws {
    let fetchCount = Counter()
    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let source = dataSource(stream.dependency(.eager)) { value in
      await fetchCount.increment()
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    source.terminate()
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await fetchCount.value == 0)
    continuation.finish()
  }

  /// Verifies that the active subscriber count stays consistent across `reset()`. Historically the
  /// teardown zeroed the count *and* the finished continuations' onTermination handlers
  /// decremented it again, driving it negative — so subscribers arriving afterwards never
  /// reached the "first subscriber" threshold and auto-refresh never started again.
  @Test("Subscriber count stays consistent after reset()")
  func subscriberCountConsistentAfterReset() async throws {
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

    let task1 = Task { for await _ in source.values {} }
    let task2 = Task { for await _ in source.values {} }
    try await Task.sleep(for: .milliseconds(20))

    source.reset()
    // Let the onTermination handlers run; they must not double-decrement the count
    try await Task.sleep(for: .milliseconds(50))
    task1.cancel()
    task2.cancel()
    let countAtTerminate = await state.fetchCount

    // A new subscriber must be recognized as the first one and restart auto-refresh
    let task3 = Task { for await _ in source.values {} }
    try await Task.sleep(for: .milliseconds(50))
    let countAfterResub = await state.fetchCount
    task3.cancel()

    #expect(countAfterResub > countAtTerminate)
  }

  /// Verifies that `terminate()` cancels an active auto-refresh timer and prevents further periodic
  /// fetches. A 20 ms auto-refresh is started with a subscriber; after observing at least one
  /// fetch, `terminate()` is called and the fetch count must remain stable across a subsequent
  /// 100 ms window — proving the timer was halted.
  @Test("terminate() cancels auto-refresh timer")
  func terminateCancelsAutoRefresh() async throws {
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
    .autoRefresh(.milliseconds(20))
    .build()

    let task = Task { for await _ in source.values {} }

    try await Task.sleep(for: .milliseconds(50))
    let countBeforeTerminate = await state.fetchCount
    #expect(countBeforeTerminate >= 1)

    source.terminate()

    try await Task.sleep(for: .milliseconds(100))
    let countAfterTerminate = await state.fetchCount
    #expect(countAfterTerminate == countBeforeTerminate)

    task.cancel()
  }

  /// Verifies that `reset()` finishes active streams and keeps the cached value, and that the
  /// source stays usable: new subscribers replay the cached value and `refresh()` fetches again
  /// (the TTL window is dropped).
  @Test("reset() finishes streams, keeps the cache, and leaves the source usable")
  func resetKeepsSourceUsable() async throws {
    let fetchCount = Counter()
    let source = dataSource {
      await fetchCount.increment()
      return "data-\(await fetchCount.value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .ttl(.seconds(60))
    .build()

    _ = try await source.refresh()
    var before = source.values.makeAsyncIterator()
    #expect(await before.next() == "data-1")

    source.reset()
    #expect(await before.next() == nil)

    var after = source.values.makeAsyncIterator()
    #expect(await after.next() == "data-1")
    #expect(try await source.refresh() == "data-2")
    #expect(await after.next() == "data-2")
  }

  /// Verifies that dependencies keep driving refreshes after `reset()`, unlike after `terminate()`.
  @Test("reset() keeps observing dependencies")
  func resetKeepsDependencies() async throws {
    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let source = dataSource(stream.dependency(.eager)) { value in
      "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    source.reset()
    continuation.yield(7)
    try await Task.sleep(for: .milliseconds(50))
    var iterator = source.values.makeAsyncIterator()
    #expect(await iterator.next() == "fetch-7")
    continuation.finish()
  }

  /// Verifies that `reset()` cancels an in-flight fetch without invoking `onError` and without
  /// clearing the cache.
  @Test("reset() cancels an in-flight fetch and keeps the cache")
  func resetCancelsInFlightFetch() async throws {
    let fetchStarted = Semaphore(value: 0)
    let errorCount = Counter()
    nonisolated(unsafe) var slow = false
    let source = dataSource { () async throws -> String in
      if !slow { return "cached" }
      await fetchStarted.signal()
      try await Task.sleep(for: .seconds(10))
      return "late"
    } onError: { _ in
      await errorCount.increment()
      return .clear
    } emptyValue: {
      "empty"
    }
    .build()

    _ = try await source.refresh()
    slow = true
    let caller = Task { try await source.refresh() }
    await fetchStarted.wait()
    source.reset()

    await #expect(throws: CancellationError.self) { try await caller.value }
    var iterator = source.values.makeAsyncIterator()
    #expect(await iterator.next() == "cached")
    #expect(await errorCount.value == 0)
  }
}
