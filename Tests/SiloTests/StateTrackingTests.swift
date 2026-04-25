import Foundation
import Silo
import Testing

@MainActor
@Suite("State Tracking Tests", .timeLimit(.minutes(1)))
struct StateTrackingTests {

  /// Verifies that the `.state` stream replays the current `DataSourceState` to a new subscriber
  /// immediately upon iteration. After a successful `refresh()`, a freshly created iterator from
  /// `.state` should yield a snapshot with `isRefreshing == false` and `isEmpty == false` without
  /// waiting for any future state change.
  @Test("State stream emits current state immediately on subscription")
  func stateStreamEmitsImmediately() async throws {
    let source = dataSource {
      "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    // New state subscription after a fetch - should get current state immediately
    var iterator = source.state.makeAsyncIterator()
    let current = await iterator.next()
    #expect(current?.isRefreshing == false)
    #expect(current?.isEmpty == false)
  }

  /// Verifies that calling `cancelRefresh()` while a fetch is in progress transitions
  /// `DataSourceState.isRefreshing` back to `false` on the `.state` stream. An in-flight fetch
  /// that sleeps for 10 seconds is cancelled; the test observes the `isRefreshing == true`
  /// emission followed immediately by a `isRefreshing == false` emission after cancellation.
  @Test("cancelRefresh sets isRefreshing to false")
  func cancelRefreshSetsNotRefreshing() async throws {
    let source = dataSource {
      try await Task.sleep(for: .seconds(10))
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var stateIterator = source.state.makeAsyncIterator()
    _ = await stateIterator.next()  // Initial: not refreshing

    Task { _ = try? await source.refresh() }

    let refreshing = await stateIterator.next()
    #expect(refreshing?.isRefreshing == true)

    source.cancelRefresh()

    let cancelled = await stateIterator.next()
    #expect(cancelled?.isRefreshing == false)
  }

  /// Verifies that `state()` (the method with configurable buffering) produces the same lifecycle
  /// transitions as the `.state` property when using default buffering. The test observes initial,
  /// refreshing, and completed states through the method-returned stream and asserts each
  /// `isRefreshing`/`isEmpty` combination matches expectations.
  @Test("state() function with default buffering matches state property")
  func stateMethodDefaultBuffering() async throws {
    let releaseFetch = Semaphore(value: 0)
    let source = dataSource {
      await releaseFetch.wait()
      return "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.state().makeAsyncIterator()

    let initial = await iterator.next()
    #expect(initial?.isRefreshing == false)
    #expect(initial?.isEmpty == true)

    Task { try? await source.refresh() }

    let refreshing = await iterator.next()
    #expect(refreshing?.isRefreshing == true)

    await releaseFetch.signal()
    let completed = await iterator.next()
    #expect(completed?.isRefreshing == false)
    #expect(completed?.isEmpty == false)
  }

  /// Verifies that `state(bufferingPolicy: .bufferingNewest(1))` drops intermediate state updates
  /// when the producer outpaces the consumer. The test subscribes with a buffer of 1 and triggers
  /// many more state transitions than the buffer can hold while deliberately blocking the consumer
  /// via a semaphore; the received snapshot count must be strictly fewer than the total
  /// transitions that occurred, proving drops happened.
  @Test("state(bufferingPolicy:) with bufferingNewest drops oldest state updates")
  func stateMethodBufferingNewest() async throws {
    actor State {
      var received: [DataSourceState] = []
      func append(_ s: DataSourceState) { received.append(s) }
    }
    let state = State()
    let unblock = Semaphore(value: 0)

    let source = dataSource {
      "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Subscribe with a 1-slot buffer. The first `next()` drains the initial snapshot;
    // the semaphore then holds the consumer while more state changes are produced.
    let consumer = Task {
      var iterator = source.state(bufferingPolicy: .bufferingNewest(1)).makeAsyncIterator()
      _ = await iterator.next()  // initial snapshot
      await state.append(DataSourceState(isRefreshing: false, isEmpty: true))
      await unblock.wait()  // pause the consumer while producers pile up

      while let snapshot = await iterator.next() {
        await state.append(snapshot)
      }
    }

    // Each refresh produces multiple state transitions (isRefreshing true → value → false).
    // With a 1-slot buffer and a blocked consumer, most must be dropped.
    let refreshCount = 20
    for _ in 0..<refreshCount {
      try await source.refresh()
    }

    source.terminate()  // finish the stream so the consumer can exit
    await unblock.signal()  // release the consumer
    await consumer.value

    let received = await state.received.count
    let totalTransitions = 1 + refreshCount * 3  // initial + (3 emissions per refresh)

    // Must have received at least the initial snapshot and some updates,
    // and strictly fewer than the total — proving drops occurred.
    #expect(received >= 2)
    #expect(received < totalTransitions)
  }

  /// Verifies that `state(bufferingPolicy: .bufferingOldest(10))` retains state emissions in
  /// arrival order, allowing the consumer to observe every transition including initial, refreshing,
  /// and completed. The large capacity ensures no drops, and the test asserts all three expected
  /// `DataSourceState` snapshots are received in sequence.
  @Test("state(bufferingPolicy:) with bufferingOldest keeps first state updates")
  func stateMethodBufferingOldest() async throws {
    let releaseFetch = Semaphore(value: 0)
    let source = dataSource {
      await releaseFetch.wait()
      return "value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // bufferingOldest(10): ample capacity to capture all state transitions
    var iterator = source.state(bufferingPolicy: .bufferingOldest(10)).makeAsyncIterator()

    let initial = await iterator.next()
    #expect(initial?.isEmpty == true)
    #expect(initial?.isRefreshing == false)

    Task { try? await source.refresh() }

    let refreshing = await iterator.next()
    #expect(refreshing?.isRefreshing == true)

    await releaseFetch.signal()
    let done = await iterator.next()
    #expect(done?.isRefreshing == false)
    #expect(done?.isEmpty == false)
  }

}
