import Foundation

/// The loading and validity state of a ``DataSource`` at a single point in time.
///
/// Emitted by ``DataSource/state`` whenever either property changes, and included in every
/// ``DataSourceValueWithState`` emitted by ``DataSource/valueWithState``.
///
/// Use this type to drive loading indicators and empty-state views:
///
/// ```swift
/// for await state in articlesSource.state {
///     loadingSpinner.isVisible  = state.isRefreshing
///     emptyView.isVisible       = state.isEmpty && !state.isRefreshing
/// }
/// ```
///
/// ## State Combinations
///
/// | `isRefreshing` | `isEmpty` | Meaning |
/// | --- | --- | --- |
/// | `false` | `true`  | Initial state — nothing fetched yet |
/// | `true`  | `true`  | First fetch in progress |
/// | `true`  | `false` | Refreshing while existing data is shown |
/// | `false` | `false` | Data loaded and idle |
public struct DataSourceState: Sendable, Equatable {
  /// `true` while a fetch task is executing.
  ///
  /// Transitions to `true` when `refresh()` begins and back to `false` when the
  /// fetch completes — whether it succeeds, fails, or is cancelled.
  public let isRefreshing: Bool

  /// `true` when the current cached value is the *empty value*.
  ///
  /// Starts as `true`. Becomes `false` after the first successful fetch.
  /// Returns to `true` when ``DataSource/clear()`` is called, when a TTL expiry
  /// clears the cache, or when `onError` returns `.clear`.
  public let isEmpty: Bool

  /// Creates a state snapshot.
  ///
  /// - Parameters:
  ///   - isRefreshing: `true` when a fetch task is executing.
  ///   - isEmpty: `true` when the cached value is the empty value.
  public init(isRefreshing: Bool, isEmpty: Bool) {
    self.isRefreshing = isRefreshing
    self.isEmpty = isEmpty
  }
}
