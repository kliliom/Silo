import Foundation
import Silo
import Testing

@MainActor
@Suite("Retry Tests", .timeLimit(.minutes(1)))
struct RetryTests {

  /// Verifies that `.retry(count:)` automatically re-invokes the fetch closure after a failure.
  /// The fetch throws for the first two attempts and succeeds on the third; the test asserts that
  /// `refresh()` ultimately returns `"success"` and that the attempt counter reached 3.
  @Test("Retry on failure")
  func retryOnFailure() async throws {
    actor State {
      var attemptCount = 0
      func increment() { attemptCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      if await state.attemptCount < 3 {
        throw NSError(domain: "test", code: 1)
      }
      return "success"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3)
    .build()

    // Should succeed after retries
    let result = try await source.refresh()
    #expect(result == "success")
    #expect(await state.attemptCount == 3)
  }

  /// Verifies that `.retry(count:delay:)` waits at least the specified duration between each
  /// attempt. Timestamps are recorded at each fetch invocation and the test asserts that the
  /// interval between the first and second attempt is at least 45 ms (allowing minor variance
  /// against the configured 50 ms delay).
  @Test("Retry with delay")
  func retryWithDelay() async throws {
    actor State {
      var attemptCount = 0
      var attemptTimes: [Date] = []
      func increment() { attemptCount += 1 }
      func appendTime(_ d: Date) { attemptTimes.append(d) }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      await state.appendTime(Date())
      if await state.attemptCount < 3 {
        throw NSError(domain: "test", code: 1)
      }
      return "success"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3, delay: .milliseconds(50))
    .build()

    try await source.refresh()

    // Verify delays between each pair of attempts
    let times = await state.attemptTimes
    #expect(times.count == 3)
    if times.count == 3 {
      let delay1 = times[1].timeIntervalSince(times[0])
      let delay2 = times[2].timeIntervalSince(times[1])
      #expect(delay1 >= 0.045)
      #expect(delay2 >= 0.045)
    }
  }

  /// Verifies that the per-attempt retry handler can stop retrying early by returning `.stop`.
  /// The fetch throws a generic error on the first attempt (handler returns `.retry`) and a
  /// `StopRetryError` on the second (handler returns `.stop`); the test asserts that the closure
  /// was invoked exactly twice and `refresh()` ultimately throws.
  @Test("Retry with error handler")
  func retryWithErrorHandler() async throws {
    actor State {
      var attemptCount = 0
      func increment() { attemptCount += 1 }
    }
    let state = State()

    struct StopRetryError: Error {}

    let source = dataSource {
      await state.increment()
      if await state.attemptCount == 1 {
        throw NSError(domain: "test", code: 1)
      } else if await state.attemptCount == 2 {
        throw StopRetryError()
      }
      return "success"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 5, delay: .milliseconds(10)) { error in
      if error is StopRetryError {
        return .stop  // Stop retrying
      }
      return .retry
    }
    .build()

    // Should stop retrying after StopRetryError
    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    #expect(await state.attemptCount == 2)  // Should stop after second attempt
  }

  /// Verifies that `RetryStrategy.exponentialBackoff` doubles the inter-attempt delay on each retry.
  /// The fetch fails for the first three attempts and succeeds on the fourth; timestamps are
  /// recorded and the test asserts that the second inter-attempt gap is strictly longer than the
  /// first, confirming the multiplier is applied correctly.
  @Test("Exponential backoff strategy")
  func exponentialBackoff() async throws {
    actor State {
      var attemptCount = 0
      var attemptTimes: [Date] = []
      func increment() { attemptCount += 1 }
      func appendTime(_ d: Date) { attemptTimes.append(d) }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      await state.appendTime(Date())
      if await state.attemptCount < 4 {
        throw NSError(domain: "test", code: 1)
      }
      return "success"
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(
      strategy: .exponentialBackoff(
        maxAttempts: 4,
        initialDelay: .milliseconds(10),
        multiplier: 2.0
      )
    )
    .build()

    try await source.refresh()

    #expect(await state.attemptCount == 4)
    #expect(await state.attemptTimes.count == 4)

    // Delays should be: ~10ms, ~20ms, ~40ms
    if await state.attemptTimes.count >= 3 {
      let times = await state.attemptTimes
      let delay1 = times[1].timeIntervalSince(times[0])
      let delay2 = times[2].timeIntervalSince(times[1])

      // Lower bounds honor the configured delays (with scheduling slack);
      // ratio check ensures the multiplier is actually applied, not just a constant.
      #expect(delay1 >= 0.008)
      #expect(delay2 >= 0.018)
      #expect(delay2 >= delay1 * 1.5)
    }
  }

  /// Verifies that `.retry(count:)` stops retrying after exactly `maxAttempts` invocations even
  /// when the fetch never succeeds. The fetch always throws; the test asserts that `refresh()`
  /// eventually rethrows and the fetch closure was called exactly 3 times (matching `count: 3`).
  @Test("Max attempts stops retry")
  func maxAttemptsStopsRetry() async throws {
    actor State {
      var attemptCount = 0
      func increment() { attemptCount += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      throw NSError(domain: "test", code: 1)  // Always fails
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3)
    .build()

    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    #expect(await state.attemptCount == 3)  // Should stop after max attempts
  }

  /// Verifies that `RetryStrategy.linearBackoff` increases the delay by a fixed increment on each
  /// retry. The fetch fails for the first two attempts and succeeds on the third; timestamps are
  /// checked to confirm the first gap is ~50 ms and the second is ~100 ms (initial delay plus one
  /// increment), within a tolerance of ~10 ms.
  @Test("Linear backoff strategy delays correctly")
  func linearBackoffStrategy() async throws {
    actor State {
      var attempts = 0
      var timestamps: [Date] = []
      func increment() { attempts += 1 }
      func appendTimestamp(_ d: Date) { timestamps.append(d) }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      await state.appendTimestamp(Date())
      if await state.attempts < 3 {
        throw NSError(domain: "Test", code: -1)
      }
      return "success"
    } onError: { _ in
      .clear
    } emptyValue: {
      "empty"
    }
    .retry(
      strategy: .linearBackoff(
        maxAttempts: 3,
        initialDelay: .milliseconds(50),
        increment: .milliseconds(50)
      )
    )
    .build()

    let result = try await source.refresh()
    #expect(result == "success")
    #expect(await state.attempts == 3)

    // Check timing: delays should be ~50ms, ~100ms
    if await state.timestamps.count == 3 {
      let ts = await state.timestamps
      let delay1 = ts[1].timeIntervalSince(ts[0])
      let delay2 = ts[2].timeIntervalSince(ts[1])

      // Only lower bounds and the relative increment are asserted; upper bounds
      // would flake on loaded CI.
      #expect(delay1 >= 0.045)
      #expect(delay2 >= 0.095)
      #expect(delay2 - delay1 >= 0.040)  // linear increment is present
    }
  }

  /// Verifies that `RetryStrategy.custom` applies caller-defined delay logic between retries.
  /// A quadratic delay formula (`attempt² × 10 ms`) is used; the test checks that the delays
  /// between the first three attempts approximate 10 ms, 40 ms, and that the second gap is more
  /// than double the first.
  @Test("Custom retry strategy with quadratic delays")
  func customRetryStrategy() async throws {
    actor State {
      var attempts = 0
      var timestamps: [Date] = []
      func increment() { attempts += 1 }
      func appendTimestamp(_ d: Date) { timestamps.append(d) }
    }
    let state = State()

    // Quadratic delays: 1^2=1ms, 2^2=4ms, 3^2=9ms, 4^2=16ms
    let strategy = RetryStrategy.custom(maxAttempts: 5) { attempt in
      let delay = attempt * attempt * 10
      return .milliseconds(Int64(delay))
    }

    let source = dataSource {
      await state.increment()
      await state.appendTimestamp(Date())
      if await state.attempts < 4 {
        throw NSError(domain: "Test", code: -1)
      }
      return "success"
    } onError: { _ in
      .clear
    } emptyValue: {
      "empty"
    }
    .retry(strategy: strategy, tolerance: .zero)
    .build()

    let result = try await source.refresh()
    #expect(result == "success")
    #expect(await state.attempts == 4)
    #expect(strategy.maxAttempts == 5)

    // Verify delays follow quadratic pattern: 10ms, 40ms, 90ms
    if await state.timestamps.count >= 3 {
      let ts = await state.timestamps
      let delay1 = ts[1].timeIntervalSince(ts[0])
      let delay2 = ts[2].timeIntervalSince(ts[1])

      // Lower bounds plus a quadratic ratio check; upper bounds would flake under load.
      #expect(delay1 >= 0.008)
      #expect(delay2 >= 0.035)
      #expect(delay2 >= delay1 * 2)
    }
  }

  /// Verifies that when the per-attempt retry handler returns `.stop` to halt retrying, the final
  /// error is still forwarded to the `onError` closure exactly once. The fetch always throws;
  /// after two attempts the handler stops retrying, and the test asserts `onErrorCallCount == 1`.
  @Test("Retry error handler .stop still invokes onError")
  func retryStopHandlerInvokesOnError() async throws {
    actor State {
      var onErrorCallCount = 0
      var attemptCount = 0
      func incrementOnError() { onErrorCallCount += 1 }
      func incrementAttempt() { attemptCount += 1 }
    }
    let state = State()

    struct StopError: Error {}

    let source = dataSource {
      await state.incrementAttempt()
      throw StopError()
    } onError: { _ in
      await state.incrementOnError()
      return .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3, delay: .milliseconds(1)) { _ in
      await state.attemptCount < 2 ? .retry : .stop
    }
    .build()

    do {
      _ = try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    #expect(await state.attemptCount == 2)
    #expect(await state.onErrorCallCount == 1)
  }

  /// Verifies that `maxAttempts` is a hard ceiling even when the per-attempt handler always returns
  /// `.retry`. The fetch always fails and the handler always signals `.retry`, but `retry(count: 3)`
  /// must still stop after exactly 3 invocations and propagate the error to the caller.
  @Test("Retry per-attempt handler returning .retry on the last attempt still exhausts and throws")
  func retryHandlerRetryOnLastAttemptStillThrows() async throws {
    actor State {
      var attempts = 0
      func increment() { attempts += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      throw NSError(domain: "test", code: 1)
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 3) { _ in
      .retry  // always request retry — but maxAttempts must still be respected
    }
    .build()

    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    #expect(await state.attempts == 3)  // exactly maxAttempts, not infinite
  }

  /// Verifies that the per-attempt retry handler can halt retrying early by returning `.stop` for
  /// a specific error type. On the first attempt a recoverable error triggers `.retry`; on the
  /// second a `CriticalError` returns `.stop`, halting the retry loop after exactly 2 attempts
  /// rather than the configured maximum of 5.
  @Test("Retry error handler with .stop halts retry")
  func retryErrorHandlerStop() async throws {
    actor State {
      var attempts = 0
      func incrementAttempts() { attempts += 1 }
    }
    let state = State()

    struct CriticalError: Error {}

    let source = dataSource {
      await state.incrementAttempts()
      if await state.attempts == 1 {
        throw NSError(domain: "Recoverable", code: 1)
      }
      throw CriticalError()
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 5, delay: .milliseconds(10)) { error in
      if error is CriticalError {
        return .stop  // Stop retrying
      }
      return .retry
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == "empty")

    // Try to refresh
    do {
      try await source.refresh()
      Issue.record("Should have thrown")
    } catch {
      // Expected
    }

    // Should have attempted twice (stopped by .stop), not all 5
    #expect(await state.attempts == 2)
  }

  /// Verifies that calling `.retry()` multiple times on a `DataSourceBuilder` applies only the
  /// last configuration, discarding all earlier ones. The builder chains `.retry(count: 10)` then
  /// `.retry(count: 2)`; the fetch always fails and the test asserts the closure was called
  /// exactly 2 times, confirming the second call won.
  @Test("Last .retry() call on the builder overrides previous retry configuration")
  func builderLastRetryWins() async throws {
    actor State {
      var attempts = 0
      func increment() { attempts += 1 }
    }
    let state = State()

    let source = dataSource {
      await state.increment()
      throw NSError(domain: "test", code: 1)
    } onError: { _ in
      .keep
    } emptyValue: {
      "empty"
    }
    .retry(count: 10)  // first call — should be overridden
    .retry(count: 2)  // last call — should win
    .build()

    do { try await source.refresh() } catch {}

    #expect(await state.attempts == 2)
  }
}
