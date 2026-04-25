import Foundation
import Silo
import Testing

/// Unit tests for `RetryStrategy` itself — no `DataSource` involvement. Verifies the pure
/// `delay(for:)` computation across the built-in factory methods.
@Suite("RetryStrategy Tests", .timeLimit(.minutes(1)))
struct RetryStrategyTests {

  /// Verifies that `RetryStrategy.exponentialBackoff` doubles the delay on each attempt when the
  /// multiplier is 2.0. The first four delays must be exactly 10 ms, 20 ms, 40 ms, and 80 ms,
  /// matching `initialDelay * multiplier^(attempt - 1)`.
  @Test("Exponential backoff produces multiplied delays")
  func exponentialBackoffDelays() {
    let strategy = RetryStrategy.exponentialBackoff(
      maxAttempts: 4,
      initialDelay: .milliseconds(10),
      multiplier: 2.0
    )

    #expect(strategy.delay(for: 1) == .milliseconds(10))
    #expect(strategy.delay(for: 2) == .milliseconds(20))
    #expect(strategy.delay(for: 3) == .milliseconds(40))
    #expect(strategy.delay(for: 4) == .milliseconds(80))
  }

  /// Verifies that the `maxDelay` parameter of `RetryStrategy.exponentialBackoff` caps computed
  /// delays so they never exceed the specified ceiling. With a 10× multiplier and a 200 ms cap,
  /// the second attempt is capped from 1 000 ms to 200 ms, and subsequent attempts remain at
  /// 200 ms rather than growing further.
  @Test("Exponential backoff maxDelay caps delay values")
  func exponentialBackoffMaxDelayCap() {
    let strategy = RetryStrategy.exponentialBackoff(
      maxAttempts: 5,
      initialDelay: .milliseconds(100),
      multiplier: 10.0,
      maxDelay: .milliseconds(200)
    )

    // Without cap: 100ms, 1000ms, 10000ms, ...
    // With cap at 200ms: 100ms, 200ms, 200ms, 200ms
    #expect(strategy.delay(for: 1) == .milliseconds(100))
    #expect(strategy.delay(for: 2) == .milliseconds(200))
    #expect(strategy.delay(for: 3) == .milliseconds(200))
    #expect(strategy.delay(for: 4) == .milliseconds(200))
  }

  /// Verifies that `RetryStrategy.linearBackoff` increases the delay by a fixed increment per
  /// attempt. Starting at 50 ms with a 25 ms increment, the first four delays must be exactly
  /// 50 ms, 75 ms, 100 ms, and 125 ms.
  @Test("Linear backoff adds a fixed increment per attempt")
  func linearBackoffDelays() {
    let strategy = RetryStrategy.linearBackoff(
      maxAttempts: 4,
      initialDelay: .milliseconds(50),
      increment: .milliseconds(25)
    )

    #expect(strategy.delay(for: 1) == .milliseconds(50))
    #expect(strategy.delay(for: 2) == .milliseconds(75))
    #expect(strategy.delay(for: 3) == .milliseconds(100))
    #expect(strategy.delay(for: 4) == .milliseconds(125))
  }

  /// Verifies that `RetryStrategy.custom` delegates delay calculation to the supplied closure and
  /// surfaces `maxAttempts` on the strategy. A quadratic formula is supplied and the test checks
  /// the computed delays for the first four attempts.
  @Test("Custom strategy delegates to the delay calculator")
  func customStrategyDelays() {
    let strategy = RetryStrategy.custom(maxAttempts: 5) { attempt in
      .milliseconds(Int64(attempt * attempt))
    }

    #expect(strategy.maxAttempts == 5)
    #expect(strategy.delay(for: 1) == .milliseconds(1))
    #expect(strategy.delay(for: 2) == .milliseconds(4))
    #expect(strategy.delay(for: 3) == .milliseconds(9))
    #expect(strategy.delay(for: 4) == .milliseconds(16))
  }
}
