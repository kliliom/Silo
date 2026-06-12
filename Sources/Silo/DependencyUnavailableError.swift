import Foundation

/// The error thrown when `refresh()` is called before every dependency has emitted a value.
///
/// A ``DataSource`` created with dependencies can only fetch once each dependency stream has
/// produced at least one value. Catch this error to distinguish "not ready yet" from real
/// fetch failures:
///
/// ```swift
/// do {
///     try await userSource.refresh()
/// } catch is DependencyUnavailableError {
///     // The user-id stream hasn't emitted yet — try again once it has
/// } catch {
///     showErrorAlert(error)
/// }
/// ```
public struct DependencyUnavailableError: Error, Sendable {
  public init() {}
}
