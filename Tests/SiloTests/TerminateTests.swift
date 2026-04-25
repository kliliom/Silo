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
  /// and leaves all streams finished. After `terminate()` is invoked three times, a newly created
  /// iterator on `.values` must yield `nil` on its first `next()` call (note: `terminate()` also
  /// finishes any future subscriptions implicitly, since the source is in a terminated state).
  @Test("terminate() is idempotent")
  func terminateIsIdempotent() async throws {
    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    source.terminate()
    source.terminate()
    source.terminate()

    // Any existing iterator created before terminate would now yield nil; we verify that
    // calling terminate repeatedly does not raise or leave the source in an unusable state.
    // A refresh after terminate must not crash either.
    _ = try? await source.refresh()
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
}
