import Foundation

/// A condition that must be satisfied before a ``DataSource`` is allowed to fetch.
///
/// Implement this protocol to create a gate that prevents fetches when a certain
/// condition is not met — for example, when the device is offline or the user is
/// not authenticated. If `check()` returns `false`, the fetch is cancelled with a
/// ``PrerequisiteError`` and no network call is made.
///
/// Register a prerequisite on a data source using
/// ``DataSourceBuilder/requires(_:)``:
///
/// ```swift
/// struct NetworkAvailable: DataSourceRefreshPrerequisite {
///     func check() async -> Bool {
///         await NetworkMonitor.shared.isConnected
///     }
/// }
///
/// let profileSource = dataSource {
///     try await api.fetchProfile()
/// } onError: { _ in .keep }
/// .requires(NetworkAvailable())
/// .build()
/// ```
///
/// Multiple prerequisites can be chained; they are evaluated in order. If one fails,
/// subsequent prerequisites are not checked.
///
/// For a comprehensive guide with common implementations, see <doc:Prerequisites>.
public protocol DataSourceRefreshPrerequisite: Sendable {
  /// Returns `true` if the fetch should proceed, `false` to abort with a ``PrerequisiteError``.
  ///
  /// Called once per settled fetch attempt — after TTL, debounce, and throttle have been
  /// evaluated — but before deduplication and the fetch closure.
  func check() async -> Bool
}

/// The error thrown when a ``DataSourceRefreshPrerequisite`` check fails.
///
/// Catch this error to distinguish prerequisite failures from network or decoding errors:
///
/// ```swift
/// do {
///     try await profileSource.refresh()
/// } catch let error as PrerequisiteError {
///     showOfflineBanner(error.message)
/// } catch {
///     showErrorAlert(error)
/// }
/// ```
public struct PrerequisiteError: Error, Sendable {
  /// Human-readable description of which prerequisite failed and why.
  public let message: String

  /// - Parameter message: Human-readable description of which prerequisite failed and why.
  public init(message: String) {
    self.message = message
  }
}
