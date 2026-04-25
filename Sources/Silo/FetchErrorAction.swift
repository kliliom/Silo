import Foundation

/// The action a ``DataSource`` should take on a fetch error.
///
/// Return this from the `onError` closure passed to `dataSource()` to control what
/// happens to the cached value when a fetch fails.
///
/// ```swift
/// dataSource {
///     try await api.fetchProfile()
/// } onError: { error in
///     switch error {
///     case is NetworkOfflineError: return .keep   // Show stale data while offline
///     case is AuthError:           return .clear  // Wipe sensitive data on auth failure
///     default:                     return .keep
///     }
/// }
/// .build()
/// ```
///
/// ## Choosing an Action
///
/// | Action | Cache after error | Use when |
/// | --- | --- | --- |
/// | `.keep` | Unchanged | Stale data is better than nothing |
/// | `.clear` | Replaced with empty value | Stale data is wrong or sensitive |
///
/// > Note: To control retry behaviour on a per-attempt basis, use ``RetryErrorAction``
/// > in the `onError` closure of ``DataSourceBuilder/retry(count:delay:tolerance:onError:)``
/// > or ``DataSourceBuilder/retry(strategy:tolerance:onError:)``. That handler decides
/// > whether to retry or stop; the cache outcome is always decided here.
public enum FetchErrorAction: Sendable {
  /// Preserve the current cached value unchanged.
  ///
  /// The error is still re-thrown to callers of `refresh()`. Use this when the
  /// cached value is still meaningful — for example, when the device goes offline
  /// and showing stale data is preferable to showing nothing.
  case keep

  /// Replace the cached value with the empty value.
  ///
  /// The empty value is emitted to all `values` and `valueWithState` subscribers.
  /// The error is still re-thrown. Use this when the cached value is no longer
  /// valid — for example, after an authentication failure that invalidates the session.
  case clear
}
