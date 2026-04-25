import Foundation
import Silo
import Testing

@MainActor
@Suite("Distinct Tests", .timeLimit(.minutes(1)))
struct DistinctTests {

  /// Verifies that `.distinct()` suppresses emissions to `.values` when the fetched value is equal
  /// to the currently cached value. Fetches the same value twice (no new emission) and then a
  /// different value, asserting that the stream advances only when the value actually changes.
  @Test("Distinct filters duplicate values")
  func distinctFiltering() async throws {
    actor State {
      var value = 1
      func setValue(_ v: Int) { value = v }
    }
    let state = State()

    let source = dataSource {
      return await state.value
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .distinct()
    .build()

    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == 1)

    // Fetch same value - should not emit
    try await source.refresh()

    // Change value - should emit
    await state.setValue(2)
    try await source.refresh()

    let second = await iterator.next()
    #expect(second == 2)
  }

  /// Verifies that `clear()` always emits the `emptyValue` to `.values` regardless of `.distinct()`,
  /// even when the current cached value equals the empty value. After fetching `42`, `clear()`
  /// resets the cache to `0`; the subsequent `refresh()` fetches `42` again and the stream emits
  /// all three transitions.
  @Test("Distinct does not suppress empty value emitted by clear()")
  func distinctClearAlwaysEmits() async throws {
    let source = dataSource {
      42
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .distinct()
    .build()

    var iterator = source.values.makeAsyncIterator()
    #expect(await iterator.next() == 0)

    try await source.refresh()
    #expect(await iterator.next() == 42)

    // clear() emits the empty value directly, bypassing distinct
    source.clear()
    #expect(await iterator.next() == 0)

    // After clear, cachedValue=0 and new fetch returns 42 — distinct allows it
    try await source.refresh()
    #expect(await iterator.next() == 42)
  }

  /// Verifies that the custom equality closure passed to `.distinct()` is invoked exactly once per
  /// completed fetch, not multiple times per emission. Three `refresh()` calls are made; the test
  /// asserts that the comparator was called exactly three times in total.
  @Test("Distinct custom comparator is called exactly once per fetch")
  func distinctComparatorCalledExactlyOncePerFetch() async throws {
    actor State {
      nonisolated(unsafe) var comparatorCallCount = 0
      nonisolated func increment() { comparatorCallCount += 1 }
    }
    let state = State()

    let source = dataSource {
      42
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .distinct { a, b in
      state.increment()
      return a == b
    }
    .build()

    try await source.refresh()  // compares 0 vs 42 → 1 call
    try await source.refresh()  // compares 42 vs 42 → 1 call
    try await source.refresh()  // compares 42 vs 42 → 1 call

    #expect(state.comparatorCallCount == 3)
  }

  /// Verifies that when `.distinct()` suppresses an emission because the fetched value is unchanged,
  /// the `DataSource` does not flip `isEmpty` back to `true`. After an initial fetch populates the
  /// cache, a second fetch returning the same value is suppressed; the `.state` stream must still
  /// report `isEmpty == false`.
  @Test("Distinct suppression does not set isEmpty back to true")
  func distinctSuppressedFetchKeepsIsEmptyFalse() async throws {
    let source = dataSource {
      42
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .distinct()
    .build()

    try await source.refresh()  // emits 42, isEmpty becomes false

    // Same value returned — distinct suppresses emission, but isEmpty must stay false
    try await source.refresh()

    var stateIterator = source.state.makeAsyncIterator()
    let current = await stateIterator.next()
    #expect(current?.isEmpty == false)
  }

  /// Verifies that a custom comparator passed to `.distinct()` is used to determine value equality,
  /// allowing domain-specific equality (e.g. comparing only `id` and `value` fields, ignoring
  /// `timestamp`). Fetches a struct twice with the same `id`/`value` (no emission) and once with a
  /// changed `id`, asserting exactly three total emissions including the initial empty value.
  @Test("Distinct with custom comparator filters correctly")
  func distinctCustomComparator() async throws {
    struct Item: Sendable {
      let id: Int
      let value: String
      let timestamp: Date
    }

    actor State {
      var emittedCount = 0
      var currentId = 1
      func incrementEmitted() { emittedCount += 1 }
      func setCurrentId(_ v: Int) { currentId = v }
    }
    let state = State()
    let emittedSem = Semaphore(value: 0)

    let source = dataSource {
      Item(id: await state.currentId, value: "data", timestamp: Date())
    } onError: { _ in
      .keep
    } emptyValue: {
      Item(id: 0, value: "empty", timestamp: Date())
    }
    .distinct { a, b in
      // Consider equal if same id and value, ignore timestamp
      a.id == b.id && a.value == b.value
    }
    .build()

    Task {
      for await _ in source.values {
        await state.incrementEmitted()
        await emittedSem.signal()
      }
    }

    // Wait for initial empty value
    await emittedSem.wait()

    // First refresh - should emit (different from empty)
    try await source.refresh()
    await emittedSem.wait()
    #expect(await state.emittedCount == 2)  // empty + first

    // Second refresh with same id/value - should NOT emit (no wait)
    try await source.refresh()
    #expect(await state.emittedCount == 2)  // No change

    // Change id - should emit
    await state.setCurrentId(2)
    try await source.refresh()
    await emittedSem.wait()
    #expect(await state.emittedCount == 3)
  }
}
