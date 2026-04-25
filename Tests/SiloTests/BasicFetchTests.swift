import Foundation
import Silo
import Testing

@MainActor
@Suite("Basic Fetch Tests", .timeLimit(.minutes(1)))
struct BasicFetchTests {

  /// Verifies the fundamental `DataSource` lifecycle: the `.values` stream emits `emptyValue`
  /// before any fetch, `refresh()` invokes the fetch closure and returns the result, and the
  /// fetched value is then emitted to all `.values` subscribers. The fetch count is asserted to
  /// confirm exactly one fetch occurred.
  @Test("Basic fetch and cache")
  func basicFetchAndCache() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      return "value-\(await state.fetchCount)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Initial value should be empty
    var iterator = source.values.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == "empty")

    // Fetch should update value
    let result = try await source.refresh()
    #expect(result == "value-1")
    #expect(await state.fetchCount == 1)

    // Should emit to stream
    let second = await iterator.next()
    #expect(second == "value-1")
    #expect(await state.fetchCount == 1)
  }

  /// Verifies that a new `.values` iterator created after a `refresh()` receives the already-cached
  /// value immediately on the first `next()` call, without requiring another fetch. This confirms
  /// the stream replays the current value to late subscribers.
  @Test("Stream emits current value immediately")
  func streamEmitsImmediately() async throws {
    let source = dataSource {
      "test-value"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    try await source.refresh()

    // New stream should get current value immediately
    var iterator = source.values.makeAsyncIterator()
    let value = await iterator.next()
    #expect(value == "test-value")
  }

  /// Verifies that concurrent `refresh()` calls are deduplicated so the fetch closure runs exactly
  /// once. Three `refresh()` calls are issued simultaneously while the fetch sleeps for 50 ms;
  /// all three must return the same result and the fetch count must equal 1.
  @Test("In-flight request deduplication")
  func inFlightDeduplication() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      try await Task.sleep(for: .milliseconds(50))
      return await state.fetchCount
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .build()

    // Start multiple refreshes concurrently
    async let result1 = source.refresh()
    async let result2 = source.refresh()
    async let result3 = source.refresh()

    let results = try await [result1, result2, result3]

    // All should get same result, only one fetch
    #expect(results == [1, 1, 1])
    #expect(await state.fetchCount == 1)
  }

  /// Verifies that a single `refresh()` broadcasts its result to all active `.values` subscribers.
  /// Three iterators are created before the fetch; after `refresh()` completes, each iterator must
  /// yield the fetched value, confirming the stream fans out to multiple consumers.
  @Test("Multiple subscribers all receive updates")
  func multipleSubscribers() async throws {
    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator1 = source.values.makeAsyncIterator()
    var iterator2 = source.values.makeAsyncIterator()
    var iterator3 = source.values.makeAsyncIterator()

    // All should have initial value
    #expect(await iterator1.next() == "empty")
    #expect(await iterator2.next() == "empty")
    #expect(await iterator3.next() == "empty")

    // Refresh
    try await source.refresh()

    // All should receive update
    #expect(await iterator1.next() == "data")
    #expect(await iterator2.next() == "data")
    #expect(await iterator3.next() == "data")
  }

  /// Verifies that the `values()` method with default unbounded buffering preserves every emitted
  /// value even when the consumer is not yet reading. Three rapid `refresh()` calls produce three
  /// distinct values; the test asserts all three are dequeued in order from the iterator,
  /// confirming nothing was dropped.
  @Test("Buffering policy - unbounded (default)")
  func bufferingPolicyUnbounded() async throws {
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
    .build()

    var iterator = source.values().makeAsyncIterator()

    // Trigger multiple rapid refreshes
    try await source.refresh()
    #expect(await state.fetchCount == 1)
    try await source.refresh()
    #expect(await state.fetchCount == 2)
    try await source.refresh()
    #expect(await state.fetchCount == 3)

    // All values should be buffered with unbounded policy
    let first = await iterator.next()
    let second = await iterator.next()
    let third = await iterator.next()
    let fourth = await iterator.next()

    #expect(first == 0)
    #expect(second == 1)
    #expect(third == 2)
    #expect(fourth == 3)
  }

  /// Verifies that `values(bufferingPolicy: .bufferingNewest(2))` drops older values when the
  /// buffer is full, keeping only the two most recent. Three rapid refreshes produce values 1, 2,
  /// and 3 while the consumer is paused; when it drains the buffer it must receive only 2 and 3.
  @Test("Buffering policy - bufferingNewest drops oldest")
  func bufferingPolicyNewest() async throws {
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
    .build()

    var iterator = source.values(bufferingPolicy: .bufferingNewest(2)).makeAsyncIterator()

    // Trigger multiple rapid refreshes
    try await source.refresh()
    #expect(await state.fetchCount == 1)
    try await source.refresh()
    #expect(await state.fetchCount == 2)
    try await source.refresh()
    #expect(await state.fetchCount == 3)

    // All values should be buffered with unbounded policy
    let first = await iterator.next()
    let second = await iterator.next()

    #expect(first == 2)
    #expect(second == 3)
  }

  /// Verifies that `values(bufferingPolicy: .bufferingOldest(2))` retains the first values
  /// received and discards later ones once the buffer is full. Three rapid refreshes produce
  /// values 1, 2, and 3; after draining, the consumer must receive the initial empty value (0)
  /// and value 1, with 2 and 3 dropped.
  @Test("Buffering policy - bufferingOldest keeps oldest")
  func bufferingPolicyOldest() async throws {
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
    .build()

    var iterator = source.values(bufferingPolicy: .bufferingOldest(2)).makeAsyncIterator()

    // Trigger multiple rapid refreshes
    try await source.refresh()
    #expect(await state.fetchCount == 1)
    try await source.refresh()
    #expect(await state.fetchCount == 2)
    try await source.refresh()
    #expect(await state.fetchCount == 3)

    // All values should be buffered with unbounded policy
    let first = await iterator.next()
    let second = await iterator.next()

    #expect(first == 0)
    #expect(second == 1)
  }
}
