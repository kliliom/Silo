import Foundation

/// A paired snapshot of a ``DataSource``'s cached value and loading state at one point in time.
///
/// Emitted by ``DataSource/valueWithState`` whenever the value or state changes, giving a
/// consistent view of both without requiring two separate subscriptions.
///
/// ```swift
/// for await snapshot in userSource.valueWithState {
///     nameLabel.text = snapshot.value.name
///     loadingIndicator.isVisible = snapshot.state.isRefreshing
/// }
/// ```
public struct DataSourceValueWithState<Value: Sendable>: Sendable {
  /// The cached value at the time of emission. Equal to `emptyValue()` when `state.isEmpty` is `true`.
  public let value: Value

  /// The loading and validity state at the time of emission.
  public let state: DataSourceState
}
