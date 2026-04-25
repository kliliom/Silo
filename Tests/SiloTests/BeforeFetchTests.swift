import Foundation
import Silo
import Testing

@MainActor
@Suite("BeforeFetch Tests", .timeLimit(.minutes(1)))
struct BeforeFetchTests {

  /// Verifies that every `.beforeFetch()` hook registered on a `DataSourceBuilder` completes
  /// before the main fetch closure is invoked. Three hooks record their identifiers into a shared
  /// set; the fetch closure checks whether all three identifiers are present and the test asserts
  /// that `fetchSawAllHooks` is `true`.
  @Test("Multiple beforeFetch hooks all run before the fetch")
  func multipleHooksAllRunBeforeFetch() async throws {
    actor State {
      var hooksSeen: Set<String> = []
      var fetchSawAllHooks = false
      func update() {
        fetchSawAllHooks = hooksSeen == ["hook-1", "hook-2", "hook-3"]
      }
      func insert(_ string: String) {
        hooksSeen.insert(string)
      }
    }
    let state = State()

    let source = dataSource {
      await state.update()
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch { await state.insert("hook-1") }
    .beforeFetch { await state.insert("hook-2") }
    .beforeFetch { await state.insert("hook-3") }
    .build()

    try await source.refresh()

    #expect(await state.fetchSawAllHooks)
  }

  /// Verifies that multiple `.beforeFetch()` hooks are executed concurrently rather than serially.
  /// Two hooks each sleep for 30 ms and track peak concurrency via an actor; the test asserts that
  /// `maxConcurrent` reached 2, confirming both hooks were in-flight simultaneously.
  @Test("Multiple beforeFetch hooks run concurrently")
  func multipleHooksRunConcurrently() async throws {
    actor State {
      var maxConcurrent = 0
      var current = 0
      func incrementCurrent() {
        current += 1
        maxConcurrent = max(maxConcurrent, current)
      }
      func decrementCurrent() { current -= 1 }
    }
    let state = State()

    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch {
      await state.incrementCurrent()
      try await Task.sleep(for: .milliseconds(30))
      await state.decrementCurrent()
    }
    .beforeFetch {
      await state.incrementCurrent()
      try await Task.sleep(for: .milliseconds(30))
      await state.decrementCurrent()
    }
    .build()

    try await source.refresh()

    #expect(await state.maxConcurrent == 2)
  }

  /// Verifies that an error thrown by a `.beforeFetch()` hook is wrapped in a `BeforeFetchError`
  /// before propagating to the caller. The test registers a hook that throws a custom `HookError`,
  /// calls `refresh()`, and asserts the caught error is a `BeforeFetchError` whose
  /// `underlyingError` is the original `HookError`.
  @Test("beforeFetch hook error is wrapped in BeforeFetchError")
  func hookErrorIsWrapped() async throws {
    struct HookError: Error {}

    let source = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch { throw HookError() }
    .build()

    do {
      try await source.refresh()
      Issue.record("Expected error")
    } catch let error as BeforeFetchError {
      #expect(error.underlyingError is HookError)
    } catch {
      Issue.record("Expected BeforeFetchError, got \(type(of: error))")
    }
  }

  /// Verifies that an error thrown by a `.beforeFetch()` hook prevents the main fetch closure from
  /// running. The hook throws unconditionally; the test calls `refresh()` (ignoring the thrown
  /// error) and asserts the fetch closure's invocation counter is still zero.
  @Test("beforeFetch hook error aborts the fetch")
  func hookErrorAbortsFetch() async throws {
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
    .beforeFetch { throw NSError(domain: "test", code: 1) }
    .build()

    do {
      try await source.refresh()
    } catch {}

    #expect(await state.fetchCount == 0)
  }

  /// Verifies that a `BeforeFetchError` produced by a failing `.beforeFetch()` hook is forwarded
  /// to the `onError` handler, allowing callers to distinguish pre-fetch failures from fetch
  /// failures. The `onError` closure checks `error is BeforeFetchError` and sets a flag; the test
  /// asserts that flag is `true` after `refresh()` throws.
  @Test("beforeFetch hook error is passed to onError handler")
  func hookErrorPassedToOnError() async throws {
    actor State {
      var onErrorReceivedBeforeFetchError = false
      func setReceived() { onErrorReceivedBeforeFetchError = true }
    }
    let state = State()

    let source = dataSource {
      "data"
    } onError: { error in
      if error is BeforeFetchError {
        await state.setReceived()
      }
      return .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch { throw NSError(domain: "test", code: 1) }
    .build()

    do {
      try await source.refresh()
    } catch {}

    #expect(await state.onErrorReceivedBeforeFetchError == true)
  }

  /// Verifies that the `.beforeFetch(refresh:)` convenience method calls `refresh()` on the
  /// supplied upstream `DataSource` before invoking the downstream fetch closure. An `order` array
  /// is populated by both closures; the test asserts that `"upstream"` appears before
  /// `"downstream"` in the recorded sequence.
  @Test("beforeFetch(refresh:) refreshes the given source first")
  func refreshConvenienceRefreshesOtherSource() async throws {
    actor State {
      var order: [String] = []
      func append(_ s: String) { order.append(s) }
    }
    let state = State()

    let upstream = dataSource {
      await state.append("upstream")
      return 42
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .build()

    let downstream = dataSource {
      await state.append("downstream")
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch(refresh: upstream)
    .build()

    try await downstream.refresh()

    #expect(await state.order == ["upstream", "downstream"])
  }

  /// Verifies that when the upstream `DataSource` passed to `.beforeFetch(refresh:)` throws during
  /// its own `refresh()`, that error is wrapped in a `BeforeFetchError` before propagating.
  /// The test asserts the caught error's `underlyingError` carries the upstream domain
  /// `"upstream"`.
  @Test("beforeFetch(refresh:) failure wraps error in BeforeFetchError")
  func refreshConvenienceWrapsError() async throws {
    let upstream = dataSource {
      throw NSError(domain: "upstream", code: 1)
    } onError: { _ in
      .keep
    } emptyValue: {
      0
    }
    .build()

    let downstream = dataSource {
      "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch(refresh: upstream)
    .build()

    do {
      try await downstream.refresh()
      Issue.record("Expected error")
    } catch let error as BeforeFetchError {
      let nsError = error.underlyingError as NSError
      #expect(nsError.domain == "upstream")
    } catch {
      Issue.record("Expected BeforeFetchError, got \(type(of: error))")
    }
  }

  /// Verifies that `.failableBeforeFetch()` silently swallows hook errors instead of aborting the
  /// fetch. The hook throws unconditionally; the test asserts that `refresh()` succeeds, returns
  /// the expected value, and that the fetch closure was invoked exactly once.
  @Test("failableBeforeFetch hook error does not abort the fetch")
  func failableHookDoesNotAbortFetch() async throws {
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
    .failableBeforeFetch { throw NSError(domain: "test", code: 1) }
    .build()

    let result = try await source.refresh()

    #expect(result == "data")
    #expect(await state.fetchCount == 1)
  }

  /// Verifies that `.failableBeforeFetch()` errors are silently discarded and never forwarded to
  /// the `onError` handler. The hook throws unconditionally, but `refresh()` completes successfully
  /// and the test asserts the `onError` closure was never invoked.
  @Test("failableBeforeFetch hook error does not reach onError handler")
  func failableHookDoesNotReachOnError() async throws {
    actor State {
      var onErrorCallCount = 0
      func increment() { onErrorCallCount += 1 }
    }
    let state = State()

    let source = dataSource {
      "data"
    } onError: { _ in
      await state.increment()
      return .keep
    } emptyValue: {
      "empty"
    }
    .failableBeforeFetch { throw NSError(domain: "test", code: 1) }
    .build()

    try await source.refresh()

    #expect(await state.onErrorCallCount == 0)
  }

  /// Verifies that concurrent `refresh()` callers that are deduplicated into a single in-flight
  /// task do not cause `.beforeFetch()` hooks or the fetch closure to run more than once. Two
  /// concurrent `refresh()` calls are fired; the second joins the in-flight task, and the test
  /// asserts both the hook counter and the fetch counter equal 1.
  @Test("beforeFetch hook runs once per actual fetch when called concurrently")
  func hookRunsOncePerFetch() async throws {
    actor State {
      var hookCount = 0
      var fetchCount = 0
      func incrementHook() { hookCount += 1 }
      func incrementFetch() { fetchCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.incrementFetch()
      try await Task.sleep(for: .milliseconds(20))
      return "data"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .beforeFetch { await state.incrementHook() }
    .build()

    async let r1 = source.refresh()
    async let r2 = source.refresh()
    _ = try await (r1, r2)

    // Second caller joins the in-flight task — hook and fetch each run once
    #expect(await state.hookCount == 1)
    #expect(await state.fetchCount == 1)
  }
}
