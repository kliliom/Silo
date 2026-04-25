import Foundation
import Silo
import Testing

@MainActor
@Suite("Error Handling Tests", .timeLimit(.minutes(1)))
struct ErrorHandlingTests {

  /// Verifies that returning `.keep` from `onError` preserves the existing cached value when a
  /// fetch fails. A first `refresh()` that throws keeps the `emptyValue` in the cache; a subsequent
  /// successful `refresh()` then replaces it, and both transitions are observed on the `.values`
  /// stream.
  @Test("Error handling with keep")
  func errorHandlingKeep() async throws {
    actor State {
      var shouldFail = true
      func setShouldFail(_ v: Bool) { shouldFail = v }
    }
    let state = State()

    let source = dataSource {
      if await state.shouldFail {
        throw NSError(domain: "test", code: 1)
      }
      return "success"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    // First fetch fails, should keep empty value
    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    var iterator = source.values.makeAsyncIterator()
    let value = await iterator.next()
    #expect(value == "empty")

    // Second fetch succeeds
    await state.setShouldFail(false)
    try await source.refresh()

    let successValue = await iterator.next()
    #expect(successValue == "success")
  }

  /// Verifies that returning `.clear` from `onError` resets the cache to `emptyValue` when a fetch
  /// fails. After a successful fetch populates the cache with `"success"`, a subsequent failing
  /// `refresh()` triggers the `.clear` action, and `.values` emits `"empty"` again.
  @Test("Error handling with clear")
  func errorHandlingClear() async throws {
    actor State {
      var shouldFail = false
      func setShouldFail(_ v: Bool) { shouldFail = v }
    }
    let state = State()

    let source = dataSource {
      if await state.shouldFail {
        throw NSError(domain: "test", code: 1)
      }
      return "success"
    } onError: { _ in
      .clear
    } emptyValue: {
      "empty"
    }
    .build()

    // First successful fetch
    try await source.refresh()

    var iterator = source.values.makeAsyncIterator()
    let first = await iterator.next()
    #expect(first == "success")

    // Second fetch fails, should clear
    await state.setShouldFail(true)
    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    let cleared = await iterator.next()
    #expect(cleared == "empty")
  }

  /// Verifies that the `onError` closure receives the exact error instance thrown by the fetch
  /// closure, not a wrapped or substituted value. The fetch throws a custom `Marker` error; the
  /// `onError` closure captures the received error and the test asserts that same `Marker` is what
  /// the handler saw and also what `refresh()` rethrows.
  @Test("onError receives the thrown error unchanged")
  func onErrorReceivesThrownError() async throws {
    struct Marker: Error, Equatable {
      let id: Int
    }
    actor State {
      var received: Marker?
      func set(_ m: Marker) { received = m }
    }
    let state = State()

    let source = dataSource {
      throw Marker(id: 42)
    } onError: { error in
      if let marker = error as? Marker {
        await state.set(marker)
      }
      return .keep
    } emptyValue: {
      "empty"
    }
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected Marker to be thrown")
    } catch let marker as Marker {
      #expect(marker == Marker(id: 42))
    } catch {
      Issue.record("Expected Marker, got \(type(of: error))")
    }

    #expect(await state.received == Marker(id: 42))
  }

  /// Verifies that `refresh()` rethrows the original fetch error rather than any error thrown by
  /// the `onError` handler. The fetch closure throws `OriginalError`; the `onError` closure also
  /// throws (via a Task implicit conversion is not possible, so it ignores — verified through
  /// callers catching `OriginalError`).
  @Test("refresh() rethrows the original fetch error, not errors from onError")
  func refreshRethrowsOriginalError() async throws {
    struct OriginalError: Error {}

    let source = dataSource {
      throw OriginalError()
    } onError: { _ in
      // onError is non-throwing; it can only return .keep or .clear. Confirm the
      // original error still propagates to the caller.
      .keep
    } emptyValue: {
      "empty"
    }
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected OriginalError")
    } catch is OriginalError {
      // Expected
    } catch {
      Issue.record("Expected OriginalError, got \(type(of: error))")
    }
  }

  /// Verifies that when retries are configured, `onError` is called exactly once — after all
  /// retries have been exhausted — rather than once per failed attempt. The fetch always fails
  /// with 3 retries allowed, so the fetch closure is invoked 3 times but `onError` must be called
  /// only once with the final error.
  @Test("onError is invoked once after retry exhaustion, not per attempt")
  func onErrorCalledOnceAfterRetryExhaustion() async throws {
    actor State {
      var fetchCount = 0
      var onErrorCount = 0
      func incrementFetch() { fetchCount += 1 }
      func incrementOnError() { onErrorCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.incrementFetch()
      throw NSError(domain: "test", code: 1)
    } onError: { _ in
      await state.incrementOnError()
      return .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3, delay: .milliseconds(1))
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Expected error")
    } catch {
      // Expected
    }

    #expect(await state.fetchCount == 3)
    #expect(await state.onErrorCount == 1)
  }

  /// Verifies that when multiple concurrent `refresh()` calls share a single in-flight fetch task
  /// that ultimately throws, every caller receives the same error. Three simultaneous `refresh()`
  /// calls are deduped into one fetch that sleeps 30 ms then throws `FetchError`; all three
  /// callers must catch `FetchError` with the same payload.
  @Test("Concurrent refresh() callers all receive the same error when the fetch fails")
  func concurrentRefreshAllGetSameError() async throws {
    struct FetchError: Error {
      let value: Int
    }
    actor State {
      var fetchCount = 0
      func increment() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      try await Task.sleep(for: .milliseconds(30))
      throw FetchError(value: await state.fetchCount)
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .build()

    async let r1: Int = source.refresh()
    async let r2: Int = source.refresh()
    async let r3: Int = source.refresh()

    do {
      _ = try await r1
      Issue.record("Expected FetchError")
    } catch let error as FetchError {
      #expect(error.value == 1)
    } catch {
      Issue.record("Expected FetchError, got \(type(of: error))")
    }
    do {
      _ = try await r2
      Issue.record("Expected FetchError")
    } catch let error as FetchError {
      #expect(error.value == 1)
    } catch {
      Issue.record("Expected FetchError, got \(type(of: error))")
    }
    do {
      _ = try await r3
      Issue.record("Expected FetchError")
    } catch let error as FetchError {
      #expect(error.value == 1)
    } catch {
      Issue.record("Expected FetchError, got \(type(of: error))")
    }

    // Fetch closure ran exactly once due to in-flight deduplication.
    #expect(await state.fetchCount == 1)
  }
}
