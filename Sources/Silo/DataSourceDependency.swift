import Foundation

/// An `AsyncStream` paired with a ``RefreshPolicy`` and clear flag, for use as a ``DataSource`` dependency.
///
/// When the stream emits a new value, the data source reacts according to the policy: refreshing
/// immediately (`.eager`), refreshing only while observed (`.lazy`), or ignoring the change (`.manual`).
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

  init(stream: AsyncStream<Value>, policy: RefreshPolicy, clear: Bool) {
    self.stream = stream
    self.policy = policy
    self.clear = clear
  }
}

extension AsyncStream where Element: Sendable {
  /// Wraps this stream as a ``DataSourceDependency`` with the given refresh policy.
  ///
  /// - Parameters:
  ///   - policy: When to trigger a refresh when this stream emits (`.eager`, `.lazy`, or `.manual`).
  ///   - clear: If `true`, clears the cached value before each dependency-triggered refresh. Default: `false`.
  ///
  /// - Returns: A dependency ready to pass to `dataSource()`.
  ///
  /// ```swift
  /// let resultsSource = dataSource(searchQuery.dependency(.eager, clear: true)) { query in
  ///     try await API.search(query: query)
  /// } onError: { _ in .keep }
  /// .debounce(.milliseconds(300))
  /// .build()
  /// ```
  public func dependency(_ policy: RefreshPolicy, clear: Bool = false) -> DataSourceDependency<Element> {
    DataSourceDependency(stream: self, policy: policy, clear: clear)
  }
}
