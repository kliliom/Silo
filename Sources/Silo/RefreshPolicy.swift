import Foundation

/// Controls when a ``DataSource`` refreshes in response to a dependency change.
///
/// Pass a policy when creating a ``DataSourceDependency`` via `.dependency(_:clear:)`:
///
/// ```swift
/// let selectedTab: AsyncStream<Tab>
///
/// let feedSource = dataSource(selectedTab.dependency(.lazy, clear: true)) { tab in
///     try await api.fetchFeed(for: tab)
/// } onError: { _ in .keep }
/// .build()
/// ```
///
/// ## Choosing a Policy
///
/// | Policy | Refresh timing | Wasted work risk | Best for |
/// | --- | --- | --- | --- |
/// | `.eager` | Immediately on any change | Higher (fetches even without viewers) | Pre-warming, always-fresh data |
/// | `.lazy` | When first subscriber arrives | Lower (fetches on demand) | On-demand screens, background tabs |
/// | `.manual` | Never automatically | None | Manual control, gated refresh |
public enum RefreshPolicy: Sendable {
  /// Refresh immediately when any dependency emits, regardless of subscriber count.
  ///
  /// Use `.eager` when you want data pre-warmed and ready before a subscriber
  /// appears — for example, when you anticipate the user navigating to a screen.
  case eager

  /// Refresh only when at least one subscriber is active on the `values` stream.
  ///
  /// If a dependency emits while there are no subscribers, the refresh is held in
  /// a pending state. When the first subscriber appears, the pending refresh fires
  /// immediately, delivering fresh data right away.
  ///
  /// Use `.lazy` for data that is only relevant when its corresponding UI is visible.
  /// This avoids unnecessary network calls for screens the user hasn't navigated to.
  case lazy

  /// Never automatically refresh in response to dependency changes.
  ///
  /// The dependency value is still available in the fetch closure — you're just
  /// responsible for calling `refresh()` at the right time. Use `.manual` when
  /// external logic should determine the refresh trigger.
  case manual
}
