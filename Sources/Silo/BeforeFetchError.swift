import Foundation

/// The error thrown when a ``DataSourceBuilder/beforeFetch(_:)`` hook fails.
///
/// When a closure registered with `.beforeFetch()` throws, the error is wrapped in a
/// `BeforeFetchError` before being passed to the `onError` handler and re-thrown to
/// the caller. This lets you distinguish pre-fetch failures from fetch failures:
///
/// ```swift
/// dataSource {
///     try await api.fetchProfile()
/// } onError: { error in
///     if let beforeFetchError = error as? BeforeFetchError {
///         // The auth token refresh failed, not the profile fetch itself
///         return .keep
///     }
///     return .clear
/// }
/// .beforeFetch { try await authSource.refresh() }
/// .build()
/// ```
public struct BeforeFetchError: Error, Sendable {
  /// The error thrown by the `beforeFetch` closure.
  public let underlyingError: any Error

  /// Creates a `BeforeFetchError` wrapping the given error.
  ///
  /// Mostly useful in tests that exercise `onError` handlers distinguishing
  /// pre-fetch failures from fetch failures.
  ///
  /// - Parameter underlyingError: The error thrown by the `beforeFetch` closure.
  public init(underlyingError: any Error) {
    self.underlyingError = underlyingError
  }
}
