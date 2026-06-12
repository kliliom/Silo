import Foundation

/// Defines how many times to retry a failed fetch and how long to wait between attempts.
///
/// Use the factory methods ``exponentialBackoff(maxAttempts:initialDelay:multiplier:maxDelay:)``,
/// ``linearBackoff(maxAttempts:initialDelay:increment:)``, or ``custom(maxAttempts:delayCalculator:)``,
/// or construct one directly with a custom delay closure.
///
/// ```swift
/// let source = dataSource {
///     try await API.getData()
/// } onError: { _ in .keep }
/// .retry(strategy: .exponentialBackoff(
///     maxAttempts: 5,
///     initialDelay: .seconds(1),
///     multiplier: 2.0
/// ))
/// .build()
/// ```
public struct RetryStrategy: Sendable {
  /// Maximum number of fetch attempts, including the first try.
  public let maxAttempts: Int

  /// Called after attempt `n` fails (`n` ranges from 1 to `maxAttempts - 1`, where 1 is the
  /// initial attempt); returns the delay to wait before attempt `n + 1`.
  public let delayCalculator: @Sendable (Int) -> Duration

  /// Creates a retry strategy with a custom delay closure.
  ///
  /// - Parameters:
  ///   - maxAttempts: Maximum number of fetch attempts, including the first try.
  ///   - delayCalculator: Called after attempt `n` fails (`n` ranges from 1 to
  ///     `maxAttempts - 1`, where 1 is the initial attempt); returns the delay to wait
  ///     before attempt `n + 1`.
  ///
  /// ```swift
  /// // 3 attempts, delays between them: 1s, 4s
  /// let strategy = RetryStrategy(maxAttempts: 3) { attempt in
  ///     .seconds(attempt * attempt)
  /// }
  /// ```
  public init(maxAttempts: Int, delayCalculator: @escaping @Sendable (Int) -> Duration) {
    self.maxAttempts = maxAttempts
    self.delayCalculator = delayCalculator
  }

  /// Returns the delay to wait after the given attempt fails, before the next attempt starts.
  ///
  /// - Parameter attemptNumber: The number of the attempt that just failed (1-indexed,
  ///   where 1 is the initial attempt).
  /// - Returns: Duration to wait before making attempt `attemptNumber + 1`.
  public func delay(for attemptNumber: Int) -> Duration {
    delayCalculator(attemptNumber)
  }

  // MARK: - Factory Methods

  /// Creates an exponential backoff strategy where delays multiply by a factor.
  ///
  /// Each retry waits longer than the previous by multiplying the previous delay.
  /// Useful for handling rate-limited APIs or overloaded servers.
  ///
  /// - Parameters:
  ///   - maxAttempts: Maximum number of fetch attempts, including the first try.
  ///     For example, `maxAttempts: 3` means one initial attempt plus up to two retries.
  ///   - initialDelay: Delay before the first retry
  ///   - multiplier: Factor to multiply delay by for each attempt (default: 2.0)
  ///   - maxDelay: Optional maximum delay cap
  ///
  /// - Returns: A retry strategy with exponential backoff
  ///
  /// Example:
  /// ```swift
  /// // 5 attempts, delays between them: 1s, 2s, 4s, 8s
  /// .exponentialBackoff(
  ///     maxAttempts: 5,
  ///     initialDelay: .seconds(1),
  ///     multiplier: 2.0
  /// )
  ///
  /// // 6 attempts, delays between them: 1s, 2s, 4s, 8s, 10s (capped)
  /// .exponentialBackoff(
  ///     maxAttempts: 6,
  ///     initialDelay: .seconds(1),
  ///     multiplier: 2.0,
  ///     maxDelay: .seconds(10)
  /// )
  /// ```
  public static func exponentialBackoff(
    maxAttempts: Int,
    initialDelay: Duration,
    multiplier: Double = 2.0,
    maxDelay: Duration? = nil
  ) -> RetryStrategy {
    RetryStrategy(maxAttempts: maxAttempts) { attemptNumber in
      // Clamp the factor so extreme attempt counts cannot overflow Duration arithmetic.
      let factor = min(pow(multiplier, Double(attemptNumber - 1)), 1e15)
      var delay = initialDelay * factor
      if let maxDelay = maxDelay, delay > maxDelay {
        delay = maxDelay
      }
      return delay
    }
  }

  /// Creates a linear backoff strategy where delays increase by a constant amount.
  ///
  /// Each retry waits a fixed increment longer than the previous.
  /// Useful for predictable retry timing.
  ///
  /// - Parameters:
  ///   - maxAttempts: Maximum number of fetch attempts, including the first try.
  ///     For example, `maxAttempts: 3` means one initial attempt plus up to two retries.
  ///   - initialDelay: Delay before the first retry
  ///   - increment: Amount to add to delay for each subsequent attempt
  ///
  /// - Returns: A retry strategy with linear backoff
  ///
  /// Example:
  /// ```swift
  /// // 4 attempts, delays between them: 1s, 3s, 5s
  /// .linearBackoff(
  ///     maxAttempts: 4,
  ///     initialDelay: .seconds(1),
  ///     increment: .seconds(2)
  /// )
  /// ```
  public static func linearBackoff(
    maxAttempts: Int,
    initialDelay: Duration,
    increment: Duration
  ) -> RetryStrategy {
    RetryStrategy(maxAttempts: maxAttempts) { attemptNumber in
      initialDelay + (increment * (attemptNumber - 1))
    }
  }

  /// Creates a custom retry strategy with a user-defined delay calculator.
  ///
  /// Allows complete control over retry timing with a custom calculation function.
  ///
  /// - Parameters:
  ///   - maxAttempts: Maximum number of fetch attempts, including the first try.
  ///     For example, `maxAttempts: 3` means one initial attempt plus up to two retries.
  ///   - delayCalculator: Called after attempt `n` fails (`n` ranges from 1 to
  ///     `maxAttempts - 1`, where 1 is the initial attempt); returns the delay to wait
  ///     before attempt `n + 1`.
  ///
  /// - Returns: A retry strategy with custom delay calculation
  ///
  /// Example:
  /// ```swift
  /// // Fibonacci backoff — 6 attempts, delays between them: 1s, 1s, 2s, 3s, 5s
  /// var prev = 0, curr = 1
  /// let strategy = .custom(maxAttempts: 6) { attempt in
  ///     let delay = curr
  ///     (prev, curr) = (curr, prev + curr)
  ///     return .seconds(delay)
  /// }
  ///
  /// // Jittered exponential backoff
  /// .custom(maxAttempts: 5) { attempt in
  ///     let base = pow(2.0, Double(attempt - 1))
  ///     let jitter = Double.random(in: 0.8...1.2)
  ///     return .seconds(base * jitter)
  /// }
  /// ```
  public static func custom(
    maxAttempts: Int,
    delayCalculator: @escaping @Sendable (Int) -> Duration
  ) -> RetryStrategy {
    RetryStrategy(maxAttempts: maxAttempts, delayCalculator: delayCalculator)
  }
}
