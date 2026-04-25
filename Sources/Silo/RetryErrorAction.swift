import Foundation

/// The action a ``DataSource`` should take after an individual retry attempt fails.
///
/// Return this from the per-attempt `onError` closure in ``DataSourceBuilder/retry(count:delay:tolerance:onError:)``
/// and ``DataSourceBuilder/retry(strategy:tolerance:onError:)`` to control whether the
/// next retry attempt should proceed.
///
/// ```swift
/// dataSource {
///     try await api.fetchArticles()
/// } onError: { _ in .keep }
/// .retry(count: 3, delay: .seconds(1)) { error in
///     switch error {
///     case is URLError:       return .retry  // Network error — try again
///     case is DecodingError:  return .stop   // Parsing error — stop retrying
///     default:                return .stop
///     }
/// }
/// .build()
/// ```
///
/// ## Choosing an Action
///
/// | Action | Effect | Use when |
/// | --- | --- | --- |
/// | `.retry` | Wait the configured delay, then attempt again | The error is transient and may succeed next time |
/// | `.stop` | Stop retrying; defer cache decision to top-level `onError` | The error is permanent and further retries would waste effort |
///
/// > Note: `.stop` does not decide what happens to the cached value — that is the
/// > responsibility of the top-level `onError` handler, which receives the final error
/// > and returns a ``FetchErrorAction`` (`.keep` or `.clear`).
public enum RetryErrorAction: Sendable {
  /// Attempt the fetch again after the configured delay.
  ///
  /// If this is the last allowed attempt, the error is thrown without waiting.
  case retry

  /// Stop retrying and let the top-level `onError` handler decide the cache outcome.
  ///
  /// The error is passed to the top-level `onError` handler — which returns a
  /// ``FetchErrorAction`` controlling whether the cache is preserved or cleared —
  /// and then re-thrown to callers of `refresh()`.
  case stop
}
