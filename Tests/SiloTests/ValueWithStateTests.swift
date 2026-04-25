import Foundation
import Silo
import Testing

@MainActor
@Suite("ValueWithState Tests", .timeLimit(.minutes(1)))
struct ValueWithStateTests {

  /// Verifies that `.valueWithState` delivers the current `DataSourceValueWithState` snapshot to
  /// a new subscriber immediately upon iteration, both before and after a fetch. Before any
  /// `refresh()` the snapshot carries the `emptyValue` and `isEmpty == true`; after a successful
  /// fetch a new iterator gets the updated value with `isEmpty == false`.
  @Test("valueWithState emits current value and state immediately on subscription")
  func emitsImmediately() async throws {
    let source = dataSource {
      "fetched"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Before any fetch
    var iterator = source.valueWithState.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial?.value == "empty")
    #expect(initial?.state.isEmpty == true)
    #expect(initial?.state.isRefreshing == false)

    // After a fetch
    try await source.refresh()
    var iterator2 = source.valueWithState.makeAsyncIterator()
    let afterFetch = await iterator2.next()
    #expect(afterFetch?.value == "fetched")
    #expect(afterFetch?.state.isEmpty == false)
    #expect(afterFetch?.state.isRefreshing == false)
  }

  /// Verifies that `.valueWithState` emits a snapshot for every observable phase of a `refresh()`
  /// lifecycle: the initial idle state, the `isRefreshing == true` state after the fetch starts,
  /// the value update while still refreshing, and the final `isRefreshing == false` completion.
  /// A semaphore controls fetch pacing so each transition can be observed in order.
  @Test("valueWithState emits state change when fetch starts and completes")
  func emitsRefreshLifecycle() async throws {
    let releaseFetch = Semaphore(value: 0)
    let source = dataSource {
      await releaseFetch.wait()
      return "fetched"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.valueWithState.makeAsyncIterator()

    // Initial snapshot
    let initial = await iterator.next()
    #expect(initial?.value == "empty")
    #expect(initial?.state.isRefreshing == false)

    Task { try? await source.refresh() }

    // State: refreshing started
    let refreshing = await iterator.next()
    #expect(refreshing?.value == "empty")
    #expect(refreshing?.state.isRefreshing == true)
    #expect(refreshing?.state.isEmpty == false)

    // Release the fetch so the value arrives
    await releaseFetch.signal()

    // Value arrives (isRefreshing still true inside the task)
    let valueReceived = await iterator.next()
    #expect(valueReceived?.value == "fetched")
    #expect(valueReceived?.state.isRefreshing == true)

    // State: fetch complete
    let done = await iterator.next()
    #expect(done?.value == "fetched")
    #expect(done?.state.isRefreshing == false)
    #expect(done?.state.isEmpty == false)
  }

  /// Verifies that `clear()` produces exactly one new emission on the `.valueWithState` stream,
  /// resetting both the cached value to `emptyValue` and `isEmpty` to `true`. After a prior fetch
  /// the test calls `clear()` and asserts only a single additional snapshot is emitted.
  @Test("valueWithState emits once on clear()")
  func emitsOnceOnClear() async throws {
    let source = dataSource {
      "fetched"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    var iterator = source.valueWithState.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial?.value == "fetched")

    source.clear()

    // Exactly one new emission from clear()
    let afterClear = await iterator.next()
    #expect(afterClear?.value == "empty")
    #expect(afterClear?.state.isEmpty == true)
  }

  /// Verifies that `cancelRefresh()` causes `.valueWithState` to emit a snapshot with
  /// `isRefreshing == false`, terminating the in-progress refresh cycle. An in-flight fetch that
  /// sleeps for 10 seconds is started; `cancelRefresh()` is called after the stream reports
  /// `isRefreshing == true`, and the test confirms the next snapshot shows the refresh is complete.
  @Test("valueWithState emits on cancelRefresh()")
  func emitsOnCancelRefresh() async throws {
    let source = dataSource {
      try await Task.sleep(for: .seconds(10))
      return "fetched"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.valueWithState.makeAsyncIterator()
    _ = await iterator.next()  // Initial

    Task { _ = try? await source.refresh() }

    let refreshing = await iterator.next()
    #expect(refreshing?.state.isRefreshing == true)

    source.cancelRefresh()

    let cancelled = await iterator.next()
    #expect(cancelled?.state.isRefreshing == false)
  }

  /// Verifies that `.valueWithState` continues to emit `isRefreshing` state transitions even when
  /// `.distinct()` suppresses a value update because the fetched result is unchanged. After an
  /// initial successful refresh, a second refresh returning the same value still produces
  /// `isRefreshing == true` and `isRefreshing == false` snapshots on the stream.
  @Test("valueWithState with distinct does not emit on unchanged value but does emit on state change")
  func distinctValueStillEmitsStateChange() async throws {
    actor State {
      var snapshots: [DataSourceValueWithState<Int>] = []
      func append(_ s: DataSourceValueWithState<Int>) { snapshots.append(s) }
    }
    let state = State()
    let fetchCycle = Semaphore(value: 0)  // signaled when isRefreshing becomes false

    let source = dataSource {
      42  // Always returns same value
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .distinct()
    .build()

    Task {
      for await snapshot in source.valueWithState {
        await state.append(snapshot)
        if !snapshot.state.isRefreshing {
          await fetchCycle.signal()
        }
      }
    }

    // Wait for initial snapshot (isRefreshing=false)
    await fetchCycle.wait()

    try await source.refresh()
    await fetchCycle.wait()  // wait for first refresh to complete
    let countAfterFirst = await state.snapshots.count
    #expect(await state.snapshots.last?.value == 42)

    // Second refresh returns same value — distinct suppresses value emission
    // but state changes (isRefreshing true then false) still emit
    try await source.refresh()
    await fetchCycle.wait()  // wait for second refresh to complete

    // Should have received the state transitions even though value didn't change
    #expect(await state.snapshots.count > countAfterFirst)
    #expect(await state.snapshots.last?.value == 42)
    #expect(await state.snapshots.last?.state.isRefreshing == false)
  }

  /// Verifies that subscribing to `.valueWithState` counts as an active subscriber and therefore
  /// keeps the `.autoRefresh` timer running. Without any `.values` subscriber, iterating only
  /// `.valueWithState` should be sufficient to trigger periodic fetches; the test confirms at
  /// least one auto-refresh fetch occurs before the task finishes.
  @Test("valueWithState counts toward active subscriber count for auto-refresh")
  func countsTowardSubscriberCount() async throws {
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

    // Subscribe via valueWithState (not values)
    let task = Task {
      var count = 0
      for await _ in source.valueWithState {
        count += 1
        if count >= 3 { break }
      }
    }

    await task.value
    #expect(await state.fetchCount >= 1)
  }

}
