import Foundation

/// An `AsyncStream` paired with a ``RefreshPolicy`` and clear/invalidate flags, for use as a ``DataSource`` dependency.
///
/// When the stream emits a new value, the data source reacts according to the policy: refreshing
/// immediately (`.eager`), refreshing only while observed (`.lazy`), or ignoring the change (`.manual`).
///
/// By default a dependency-triggered refresh goes through the same TTL and throttle gates as a
/// manual one, so a change inside a TTL window keeps serving the value fetched for the previous
/// dependency value. Pass `invalidate: true` to drop those windows whenever the dependency changes.
///
/// Create a dependency with the `.dependency(_:clear:)` extension on `AsyncStream`:
///
/// ```swift
/// let userSource = dataSource(userIdStream.dependency(.eager)) { userId in
///     try await API.getUser(id: userId)
/// } onError: { _ in
///     .keep
/// } emptyValue: {
///     User.placeholder
/// }
/// .build()
/// ```
public struct DataSourceDependency<Value: Sendable>: Sendable {
  let stream: AsyncStream<Value>
  let policy: RefreshPolicy
  let clear: Bool
  let invalidate: Bool

  init(stream: AsyncStream<Value>, policy: RefreshPolicy, clear: Bool, invalidate: Bool) {
    self.stream = stream
    self.policy = policy
    self.clear = clear
    self.invalidate = invalidate
  }
}

extension AsyncStream where Element: Sendable {
  /// Wraps this stream as a ``DataSourceDependency`` with the given refresh policy.
  ///
  /// - Parameters:
  ///   - policy: When to trigger a refresh when this stream emits (`.eager`, `.lazy`, or `.manual`).
  ///   - clear: If `true`, clears the cached value before each dependency-triggered refresh. Default: `false`.
  ///   - invalidate: If `true`, each emission drops the TTL and throttle windows, so the next refresh
  ///     fetches data for the new value. Unlike `clear`, the cached value stays visible until the new
  ///     fetch lands. Applies to every policy — with `.manual`, the next manual `refresh()` fetches.
  ///     Default: `false`, where TTL and throttle gate dependency-triggered refreshes like any other.
  ///
  /// - Returns: A dependency ready to pass to `dataSource()`.
  ///
  /// ```swift
  /// let resultsSource = dataSource(searchQuery.dependency(.eager, clear: true)) { query in
  ///     try await API.search(query: query)
  /// } onError: { _ in .keep }
  /// .debounce(.milliseconds(300))
  /// .build()
  ///
  /// // Cached for 5 minutes, but a new user ID always fetches right away
  /// let profileSource = dataSource(userId.dependency(.eager, invalidate: true)) { id in
  ///     try await API.getProfile(id: id)
  /// } onError: { _ in .keep }
  /// .ttl(.seconds(300))
  /// .build()
  /// ```
  public func dependency(
    _ policy: RefreshPolicy,
    clear: Bool = false,
    invalidate: Bool = false
  ) -> DataSourceDependency<Element> {
    DataSourceDependency(stream: self, policy: policy, clear: clear, invalidate: invalidate)
  }
}
