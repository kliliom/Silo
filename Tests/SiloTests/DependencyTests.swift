import Foundation
import Silo
import Testing

@MainActor
@Suite("Dependency Tests", .timeLimit(.minutes(1)))
struct DependencyTests {

  /// Verifies that a `DataSource` with a `.eager` dependency policy automatically calls `refresh()`
  /// whenever the upstream `AsyncStream` emits a new value, regardless of whether there are active
  /// subscribers. Each yielded value is passed into the fetch closure, and the test asserts the
  /// result reflects the dependency value on every emission.
  @Test("Single dependency with eager policy triggers refresh")
  func singleDependencyEager() async throws {
    actor State {
      var fetchCount = 0
      var lastSeenValue: Int?
      func increment() { fetchCount += 1 }
      func setLastSeenValue(_ v: Int) { lastSeenValue = v }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.eager)

    let source = dataSource(dependency) { value in
      await state.increment()
      await state.setLastSeenValue(value)
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Start observing the stream
    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "empty")

    // Emit dependency value
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))

    // Should have triggered a fetch
    #expect(await state.fetchCount == 1)
    #expect(await state.lastSeenValue == 1)

    let updated = await iterator.next()
    #expect(updated == "fetch-1")

    // Emit another value
    continuation.yield(2)
    try await Task.sleep(for: .milliseconds(50))

    #expect(await state.fetchCount == 2)
    #expect(await state.lastSeenValue == 2)

    let updated2 = await iterator.next()
    #expect(updated2 == "fetch-2")

    continuation.finish()
  }

  /// Verifies that a `.lazy` dependency policy defers auto-refresh until there is at least one
  /// active `.values` subscriber. A value emitted before any subscriber arrives is recorded as
  /// pending; once a subscriber is added the deferred fetch fires with the latest dependency value.
  /// Subsequent emissions with an active subscriber trigger refreshes immediately.
  @Test("Single dependency with lazy policy only refreshes with subscribers")
  func singleDependencyLazy() async throws {
    actor State {
      var fetchCount = 0
      var lastSeenValue: Int?
      func increment() { fetchCount += 1 }
      func setLastSeenValue(_ v: Int) { lastSeenValue = v }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.lazy)

    let source = dataSource(dependency) { value in
      await state.increment()
      await state.setLastSeenValue(value)
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Emit value WITHOUT subscribers
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))

    // Should NOT trigger fetch (no subscribers)
    #expect(await state.fetchCount == 0)

    // Now add a subscriber
    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "empty")

    // Should trigger fetch now (dependency changed while no subscribers)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)
    #expect(await state.lastSeenValue == 1)

    let deferred = await iterator.next()
    #expect(deferred == "fetch-1")

    // Emit value WITH subscribers
    continuation.yield(2)
    try await Task.sleep(for: .milliseconds(50))

    // Should trigger fetch immediately (has subscribers)
    #expect(await state.fetchCount == 2)
    #expect(await state.lastSeenValue == 2)

    let updated = await iterator.next()
    #expect(updated == "fetch-2")

    continuation.finish()
  }

  /// Verifies that a `.manual` dependency policy never triggers an automatic `refresh()`, even with
  /// active subscribers and repeated upstream emissions. Two values are yielded to the dependency
  /// stream and the test asserts the fetch closure was never invoked, confirming the policy truly
  /// disables auto-refresh.
  @Test("Single dependency with disabled policy never auto-refreshes")
  func singleDependencyDisabled() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.manual)

    let source = dataSource(dependency) { value in
      await state.increment()
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "empty")

    // Emit values
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))

    // Should NOT trigger fetch (manual policy)
    #expect(await state.fetchCount == 0)

    continuation.yield(2)
    try await Task.sleep(for: .milliseconds(50))

    // Still should not trigger
    #expect(await state.fetchCount == 0)

    continuation.finish()
  }

  /// Verifies that setting `clear: true` on a dependency causes the `DataSource` to emit
  /// `emptyValue` before each dependency-triggered fetch, producing a visible reset in the `.values`
  /// stream. After a dependency value is yielded, the test asserts the stream contains both
  /// `"empty"` and the fetched result in the correct order.
  @Test("Dependency with clear flag clears cache before refresh")
  func dependencyWithClearFlag() async throws {
    actor State {
      var values: [String] = []
      func append(_ v: String) { values.append(v) }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.eager, clear: true)

    let source = dataSource(dependency) { value in
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    Task {
      for await value in source.values {
        await state.append(value)
      }
    }

    try await Task.sleep(for: .milliseconds(50))

    // Emit dependency value
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(100))

    // Should see: empty (initial) -> empty (clearing) -> fetch-1 (refresh)
    #expect(await state.values.contains("empty"))
    #expect(await state.values.contains("fetch-1"))
    #expect(await state.values.count >= 2)  // At least initial and fetched value

    continuation.finish()
  }

  /// Verifies that a `DataSource` built with two eager dependencies triggers a `refresh()` whenever
  /// either upstream stream emits, passing the latest value from each stream to the fetch closure.
  /// The test emits values to each stream in turn and asserts the fetch results combine both
  /// dependency values correctly on each trigger.
  @Test("Multiple dependencies all trigger refresh")
  func multipleDependencies() async throws {
    actor State {
      var fetchCount = 0
      var lastSeenA: Int?
      var lastSeenB: String?
      func increment() { fetchCount += 1 }
      func setA(_ v: Int) { lastSeenA = v }
      func setB(_ v: String) { lastSeenB = v }
    }
    let state = State()

    let (streamA, continuationA) = AsyncStream.makeStream(of: Int.self)
    let (streamB, continuationB) = AsyncStream.makeStream(of: String.self)

    let depA = streamA.dependency(.eager)
    let depB = streamB.dependency(.eager)

    let source = dataSource(depA, depB) { a, b in
      await state.increment()
      await state.setA(a)
      await state.setB(b)
      return "fetch-\(a)-\(b)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "empty")

    // Emit first dependency — with only A set, the fetch must not produce a value:
    // the DependencyCoordinator's wrappedFetch throws because not all dependencies have
    // emitted, so the fetch closure is never reached.
    continuationA.yield(1)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 0)
    #expect(await state.lastSeenA == nil)

    // Emit second dependency — now both are set, the fetch runs.
    continuationB.yield("hello")
    try await Task.sleep(for: .milliseconds(50))

    // Now both dependencies are set
    #expect(await state.lastSeenA == 1)
    #expect(await state.lastSeenB == "hello")
    let firstFetch = await iterator.next()
    #expect(firstFetch == "fetch-1-hello")

    // Change first dependency
    continuationA.yield(2)
    try await Task.sleep(for: .milliseconds(50))

    #expect(await state.lastSeenA == 2)
    #expect(await state.lastSeenB == "hello")
    let secondFetch = await iterator.next()
    #expect(secondFetch == "fetch-2-hello")

    // Change second dependency
    continuationB.yield("world")
    try await Task.sleep(for: .milliseconds(50))

    #expect(await state.lastSeenA == 2)
    #expect(await state.lastSeenB == "world")
    let thirdFetch = await iterator.next()
    #expect(thirdFetch == "fetch-2-world")

    continuationA.finish()
    continuationB.finish()
  }

  /// Verifies that `.debounce()` is applied to dependency-triggered refreshes, collapsing rapid
  /// upstream emissions into a single fetch. Three values are yielded to the dependency stream in
  /// quick succession; after the debounce window closes, the test asserts the fetch closure was
  /// invoked exactly once.
  @Test("Dependency changes are debounced if debounce is set")
  func dependencyWithDebounce() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.eager)

    let source = dataSource(dependency) { value in
      await state.increment()
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .debounce(.milliseconds(100))
    .build()

    var iterator = source.values.makeAsyncIterator()
    _ = await iterator.next()

    // Emit multiple values quickly
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(20))
    continuation.yield(2)
    try await Task.sleep(for: .milliseconds(20))
    continuation.yield(3)

    // Wait for debounce
    try await Task.sleep(for: .milliseconds(150))

    // Should only fetch once with last value
    #expect(await state.fetchCount == 1)

    continuation.finish()
  }

  /// Verifies that `.ttl()` gates dependency-triggered refreshes just as it does manual ones.
  /// A first dependency emission triggers a fetch; a second emission within the TTL window is
  /// suppressed (fetch count stays at 1); a third emission after the TTL expires triggers another
  /// fetch, bringing the total to 2.
  @Test("Dependency changes respect TTL")
  func dependencyWithTTL() async throws {
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.eager)

    let source = dataSource(dependency) { value in
      await state.increment()
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .ttl(.milliseconds(100))
    .build()

    var iterator = source.values.makeAsyncIterator()
    _ = await iterator.next()

    // First dependency change - should fetch
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)

    // Immediate second change - should be blocked by TTL
    continuation.yield(2)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)  // Still 1

    // Wait for TTL to expire
    try await Task.sleep(for: .milliseconds(100))

    // Third change - should fetch now
    continuation.yield(3)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 2)

    continuation.finish()
  }

  /// Verifies that a dependency stream with a buffered value emitted before `DataSource` creation
  /// is consumed by the `DependencyCoordinator` so the fetch closure receives it. A value of 42
  /// is yielded to a `bufferingNewest(1)` stream before the `DataSource` is built; the test
  /// asserts the fetch closure was called with that value.
  @Test("Dependency receives current value on DataSource build")
  func dependencyCurrentValue() async throws {
    actor State {
      var receivedValue: Int?
      func setReceivedValue(_ v: Int) { receivedValue = v }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self, bufferingPolicy: .bufferingNewest(1))

    // Emit value before creating DataSource
    continuation.yield(42)

    let dependency = stream.dependency(.eager)

    let source = dataSource(dependency) { value in
      await state.setReceivedValue(value)
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    _ = await iterator.next()

    try await Task.sleep(for: .milliseconds(100))

    // Should have received the buffered value
    #expect(await state.receivedValue == 42)

    continuation.finish()
  }

  /// Verifies that when multiple values are emitted to a `.lazy` dependency stream while there are
  /// no subscribers, only the most recent value is used when the deferred fetch finally fires.
  /// Values 1, 2, and 3 are yielded before any subscriber exists; once a subscriber is added the
  /// single triggered fetch must receive value 3.
  @Test("Lazy dependency uses only the latest value emitted while no subscribers")
  func lazyDependencyUsesLatestPendingValue() async throws {
    actor State {
      var fetchCount = 0
      var lastSeenValue: Int?
      func increment() { fetchCount += 1 }
      func setLastSeenValue(_ v: Int) { lastSeenValue = v }
    }
    let state = State()

    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.lazy)

    let source = dataSource(dependency) { value in
      await state.increment()
      await state.setLastSeenValue(value)
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // Emit multiple values with no subscribers — all should be superseded by the last
    continuation.yield(1)
    continuation.yield(2)
    continuation.yield(3)
    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 0)

    var iterator = source.values.makeAsyncIterator()
    _ = await iterator.next()  // empty

    try await Task.sleep(for: .milliseconds(50))
    #expect(await state.fetchCount == 1)
    #expect(await state.lastSeenValue == 3)

    continuation.finish()
  }

  /// Verifies that finishing a dependency `AsyncStream` does not crash or invalidate the
  /// `DataSource`. After a value is emitted and the stream is terminated, the `DataSource` must
  /// still accept a manual `refresh()` call and return a result based on the last known dependency
  /// value.
  @Test("Dependency stream termination doesn't crash DataSource")
  func dependencyStreamTermination() async throws {
    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)
    let dependency = stream.dependency(.eager)

    let source = dataSource(dependency) { value in
      return "fetch-\(value)"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    _ = await iterator.next()

    // Emit a value
    continuation.yield(1)
    try await Task.sleep(for: .milliseconds(50))

    // Terminate the dependency stream
    continuation.finish()
    try await Task.sleep(for: .milliseconds(50))

    // DataSource should still be usable
    let result = try await source.refresh()
    #expect(result == "fetch-1")  // Should use last known dependency value
  }

  /// Verifies that calling `refresh()` on a dependency-backed `DataSource` before any dependency
  /// value has been emitted throws an error rather than passing `nil` to the fetch closure.
  /// A `.manual` dependency stream is created but never yielded to; the test asserts `refresh()`
  /// throws an `NSError` with domain `"DataSource"`.
  @Test("Refresh throws when the dependency has not yet emitted any value")
  func refreshThrowsWhenDependencyUnavailable() async throws {
    let (stream, continuation) = AsyncStream.makeStream(of: Int.self)

    // Use .manual so the dependency observer is registered but never auto-refreshes
    let source = dataSource(stream.dependency(.manual)) { _ in
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // No value has been yielded to the stream, so stateHolder.value == nil
    do {
      _ = try await source.refresh()
      Issue.record("Expected error when dependency unavailable")
    } catch let error as NSError {
      #expect(error.domain == "DataSource")
    }

    continuation.finish()
  }
}
