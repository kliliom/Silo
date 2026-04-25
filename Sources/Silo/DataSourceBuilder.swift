import Foundation

// MARK: - Builder for DataSource

/// Creates a DataSource builder with optional dependencies.
///
/// This function creates a builder for a reactive data source that can depend on upstream
/// async streams. When dependencies emit new values, the data source can automatically
/// refresh based on the configured refresh policy.
///
/// - Parameters:
///   - dependency: Zero or more dependencies that the fetch operation depends on
///   - fetch: Async closure that fetches data, receiving dependency values as parameters
///   - onError: Called on fetch failure; return `.keep` or `.clear`. Defaults to `.clear`.
///   - emptyValue: Closure that provides the value when data is empty
///
/// - Returns: A builder for configuring and creating the DataSource
///
/// Example without dependencies:
/// ```swift
/// let source = dataSource {
///     try await API.getData()
/// } onError: { error in
///     .keep
/// } emptyValue: {
///     "placeholder"
/// }
/// .ttl(.seconds(300))
/// .build()
/// ```
///
/// Example with dependencies:
/// ```swift
/// let userIdStream = AsyncStream<Int> { ... }
/// let userSource = dataSource(userIdStream.dependency(.eager)) { userId in
///     try await API.getUser(id: userId)
/// } onError: { _ in
///     .keep
/// } emptyValue: {
///     User.placeholder
/// }
/// .build()
/// ```
@MainActor
public func dataSource<Value: Sendable, each Dependency: Sendable>(
  _ dependency: repeat DataSourceDependency<each Dependency>,
  fetch: @escaping @Sendable (repeat each Dependency) async throws -> Value,
  onError: @escaping @Sendable (Error) async -> FetchErrorAction = { _ in return .clear },
  emptyValue: @escaping @Sendable () -> Value
) -> DataSourceBuilder<Value> {
  DataSourceBuilder(
    dependency: repeat each dependency,
    fetch: fetch,
    errorHandler: onError,
    emptyValue: emptyValue
  )
}

/// Creates a DataSource builder for optional values.
///
/// Convenience function for types that can be expressed as `nil`. The `emptyValue`
/// closure automatically returns `nil` for the empty state.
///
/// - Parameters:
///   - dependency: Zero or more dependencies that the fetch operation depends on
///   - fetch: Async closure that fetches data, receiving dependency values as parameters
///   - onError: Called on fetch failure; return `.keep` or `.clear`. Defaults to `.clear`.
///
/// - Returns: A builder for configuring and creating the DataSource
///
/// Example:
/// ```swift
/// let source = dataSource {
///     try await API.getUser() // Returns User?
/// } onError: { _ in
///     .keep
/// }
/// .build()
/// // Empty value is automatically nil
/// ```
@MainActor
public func dataSource<Value: Sendable & ExpressibleByNilLiteral, each Dependency: Sendable>(
  _ dependency: repeat DataSourceDependency<each Dependency>,
  fetch: @escaping @Sendable (repeat each Dependency) async throws -> Value,
  onError: @escaping @Sendable (Error) async -> FetchErrorAction = { _ in return .clear }
) -> DataSourceBuilder<Value> {
  DataSourceBuilder(
    dependency: repeat each dependency,
    fetch: fetch,
    errorHandler: onError,
    emptyValue: { nil }
  )
}

/// Creates a DataSource builder for array values.
///
/// Convenience function for arrays. The `emptyValue` closure automatically
/// returns an empty array for the empty state.
///
/// - Parameters:
///   - dependency: Zero or more dependencies that the fetch operation depends on
///   - fetch: Async closure that fetches data as an array, receiving dependency values as parameters
///   - onError: Called on fetch failure; return `.keep` or `.clear`. Defaults to `.clear`.
///
/// - Returns: A builder for configuring and creating the DataSource
///
/// Example:
/// ```swift
/// let source = dataSource {
///     try await API.getUsers() // Returns [User]
/// } onError: { _ in
///     .keep
/// }
/// .build()
/// // Empty value is automatically []
/// ```
@MainActor
public func dataSource<Value: Sendable, each Dependency: Sendable>(
  _ dependency: repeat DataSourceDependency<each Dependency>,
  fetch: @escaping @Sendable (repeat each Dependency) async throws -> [Value],
  onError: @escaping @Sendable (Error) async -> FetchErrorAction = { _ in return .clear }
) -> DataSourceBuilder<[Value]> {
  DataSourceBuilder(
    dependency: repeat each dependency,
    fetch: fetch,
    errorHandler: onError,
    emptyValue: { [] }
  )
}

/// Creates a DataSource builder for dictionary values.
///
/// Convenience function for dictionaries. The `emptyValue` closure automatically
/// returns an empty dictionary for the empty state.
///
/// - Parameters:
///   - dependency: Zero or more dependencies that the fetch operation depends on
///   - fetch: Async closure that fetches data as a dictionary, receiving dependency values as parameters
///   - onError: Called on fetch failure; return `.keep` or `.clear`. Defaults to `.clear`.
///
/// - Returns: A builder for configuring and creating the DataSource
///
/// Example:
/// ```swift
/// let source = dataSource {
///     try await API.getUserSettings() // Returns [String: Any]
/// } onError: { _ in
///     .keep
/// }
/// .build()
/// // Empty value is automatically [:]
/// ```
@MainActor
public func dataSource<Key: Sendable & Hashable, Value: Sendable, each Dependency: Sendable>(
  _ dependency: repeat DataSourceDependency<each Dependency>,
  fetch: @escaping @Sendable (repeat each Dependency) async throws -> [Key: Value],
  onError: @escaping @Sendable (Error) async -> FetchErrorAction = { _ in return .clear }
) -> DataSourceBuilder<[Key: Value]> {
  DataSourceBuilder(
    dependency: repeat each dependency,
    fetch: fetch,
    errorHandler: onError,
    emptyValue: { [:] }
  )
}

// MARK: - Builder Classes

/// Fluent builder for configuring a ``DataSource``.
///
/// Obtain one from the ``dataSource(_:fetch:onError:emptyValue:)`` function, chain configuration
/// methods, then call ``build()`` to create the ``DataSource``.
///
/// ```swift
/// let source = dataSource {
///     try await API.getData()
/// } onError: { _ in
///     .keep
/// } emptyValue: {
///     Data.empty
/// }
/// .ttl(.seconds(300))
/// .throttle(.seconds(1))
/// .retry(count: 3)
/// .distinct()
/// .build()
/// ```
@MainActor
public final class DataSourceBuilder<Value: Sendable>: Sendable {
  private let stateHolder: any DependencyCoordinating
  private let fetch: @Sendable () async throws -> Value
  private let errorHandler: @Sendable (Error) async -> FetchErrorAction
  private let emptyValue: @Sendable () -> Value

  private var beforeFetchHandlers: [@Sendable () async throws -> Void] = []
  private var distinctComparator: (@Sendable (Value, Value) -> Bool)?
  private var ttlDuration: Duration?
  private var ttlClear: Bool = false
  private var ttlTolerance: Duration?
  private var throttleDuration: Duration?
  private var throttleLast: Bool = false
  private var throttleTolerance: Duration?
  private var debounceDuration: Duration?
  private var debounceTolerance: Duration?
  private var autoRefreshInterval: Duration?
  private var autoRefreshTolerance: Duration?
  private var retryStrategy: RetryStrategy?
  private var retryErrorHandler: (@Sendable (Error) async -> RetryErrorAction)?
  private var retryTolerance: Duration?
  private var prerequisites: [DataSourceRefreshPrerequisite] = []

  init<each Dependency: Sendable>(
    dependency: repeat DataSourceDependency<each Dependency>,
    fetch: @escaping @Sendable (repeat each Dependency) async throws -> Value,
    errorHandler: @escaping @Sendable (Error) async -> FetchErrorAction,
    emptyValue: @escaping @Sendable () -> Value
  ) {
    let stateHolder = DependencyCoordinator(dependency: repeat each dependency)
    self.stateHolder = stateHolder

    let wrappedFetch: @Sendable () async throws -> Value = { @MainActor [fetch] in
      guard let setValues = stateHolder.value else {
        throw NSError(
          domain: "DataSource",
          code: -1,
          userInfo: [NSLocalizedDescriptionKey: "Dependencies not yet available"]
        )
      }
      return try await fetch(repeat each setValues)
    }

    self.fetch = wrappedFetch
    self.errorHandler = errorHandler
    self.emptyValue = emptyValue
  }

  /// Configure time-to-live (TTL) caching.
  ///
  /// Data is considered fresh for the specified duration after a successful fetch.
  /// During this window, `refresh()` returns the cached value without fetching.
  ///
  /// - Parameters:
  ///   - duration: How long cached data remains valid
  ///   - tolerance: Allowed deviation in the TTL expiry timer, passed to `Task.sleep`.
  ///     The system may fire the timer up to this amount later than requested, allowing
  ///     it to coalesce wake-ups for efficiency. `nil` uses the system default (default: `nil`)
  ///   - clear: If `true`, automatically clears data when TTL expires (default: `false`)
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource { ... }
  ///     .ttl(.seconds(300)) // Cache for 5 minutes
  ///     .build()
  ///
  /// dataSource { ... }
  ///     .ttl(.seconds(300), clear: true) // Auto-clear after 5 minutes
  ///     .build()
  ///
  /// dataSource { ... }
  ///     .ttl(.seconds(300), tolerance: .seconds(10)) // Allow up to 10s late expiry
  ///     .build()
  /// ```
  public func ttl(_ duration: Duration, tolerance: Duration? = nil, clear: Bool = false) -> Self {
    self.ttlDuration = duration
    self.ttlTolerance = tolerance
    self.ttlClear = clear
    return self
  }

  /// Configure request throttling to limit fetch frequency.
  ///
  /// Throttling prevents excessive fetching by enforcing a minimum time between requests.
  ///
  /// - Parameters:
  ///   - duration: Minimum time that must elapse between fetches
  ///   - tolerance: Allowed deviation when waiting for the throttle window to end
  ///     (only relevant when `last: true`), passed to `Task.sleep`.
  ///     `nil` uses the system default (default: `nil`)
  ///   - last: If `true`, queues and executes the last request after throttle expires;
  ///           if `false`, ignores requests during throttle window (default: `false`)
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// // Ignore rapid-fire requests
  /// dataSource { ... }
  ///     .throttle(.seconds(1))
  ///     .build()
  ///
  /// // Queue the last request
  /// dataSource { ... }
  ///     .throttle(.seconds(1), last: true)
  ///     .build()
  /// ```
  public func throttle(_ duration: Duration, tolerance: Duration? = nil, last: Bool = false) -> Self {
    self.throttleDuration = duration
    self.throttleLast = last
    self.throttleTolerance = tolerance
    return self
  }

  /// Configure request debouncing to delay fetches until calls stop.
  ///
  /// Debouncing waits for a quiet period before executing a fetch. If another
  /// `refresh()` is called during the wait, the timer resets.
  ///
  /// - Parameters:
  ///   - duration: How long to wait after the last call before fetching
  ///   - tolerance: Allowed deviation in the debounce wait timer, passed to `Task.sleep`.
  ///     `nil` uses the system default (default: `nil`)
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// // Wait 300ms after user stops typing before searching
  /// dataSource { searchQuery in
  ///     try await API.search(query: searchQuery)
  /// }
  /// .debounce(.milliseconds(300))
  /// .build()
  /// ```
  public func debounce(_ duration: Duration, tolerance: Duration? = nil) -> Self {
    self.debounceDuration = duration
    self.debounceTolerance = tolerance
    return self
  }

  /// Configure automatic periodic refreshing.
  ///
  /// When configured, the data source automatically refreshes at regular intervals
  /// while there are active subscribers to the `values` stream. The timer stops
  /// when the last subscriber terminates.
  ///
  /// - Parameters:
  ///   - interval: Time between automatic refreshes
  ///   - tolerance: Allowed deviation in the refresh interval timer, passed to `Task.sleep`.
  ///     Larger tolerances improve battery life by letting the system coalesce wake-ups.
  ///     `nil` uses the system default (default: `nil`)
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// // Refresh every minute while being observed
  /// dataSource { ... }
  ///     .autoRefresh(.seconds(60))
  ///     .build()
  ///
  /// // Allow up to 5s late to improve efficiency
  /// dataSource { ... }
  ///     .autoRefresh(.seconds(60), tolerance: .seconds(5))
  ///     .build()
  /// ```
  public func autoRefresh(_ interval: Duration, tolerance: Duration? = nil) -> Self {
    self.autoRefreshInterval = interval
    self.autoRefreshTolerance = tolerance
    return self
  }

  /// Configure simple retry with optional delay.
  ///
  /// Retries failed fetches up to the specified count with a constant delay between attempts.
  ///
  /// - Parameters:
  ///   - count: Maximum number of retry attempts
  ///   - delay: Time to wait between attempts (default: `.zero`)
  ///   - tolerance: Allowed deviation in each retry delay timer, passed to `Task.sleep`.
  ///     `nil` uses the system default (default: `nil`)
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource { ... }
  ///     .retry(count: 3, delay: .seconds(1))
  ///     .build()
  /// ```
  public func retry(count: Int, delay: Duration = .zero, tolerance: Duration? = nil) -> Self {
    self.retryStrategy = .exponentialBackoff(maxAttempts: count, initialDelay: delay, multiplier: 1.0)
    self.retryTolerance = tolerance
    self.retryErrorHandler = nil
    return self
  }

  /// Configure simple retry with per-attempt error handling.
  ///
  /// Retries failed fetches with a custom error handler that decides, after each
  /// individual attempt, whether to retry or stop. The cache outcome on a terminal
  /// failure is decided by the top-level `onError` handler, not this one.
  ///
  /// - Parameters:
  ///   - count: Maximum number of retry attempts
  ///   - delay: Time to wait between attempts (default: `.zero`)
  ///   - tolerance: Allowed deviation in each retry delay timer, passed to `Task.sleep`.
  ///     `nil` uses the system default (default: `nil`)
  ///   - onError: Closure returning a ``RetryErrorAction`` that controls whether to retry this error
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource { ... }
  ///     .retry(count: 3, delay: .seconds(1)) { error in
  ///         if error is URLError {
  ///             return .retry  // Retry transient network errors
  ///         }
  ///         return .stop       // Stop retrying for anything else
  ///     }
  ///     .build()
  /// ```
  public func retry(
    count: Int,
    delay: Duration = .zero,
    tolerance: Duration? = nil,
    onError: @escaping @Sendable (Error) async -> RetryErrorAction
  ) -> Self {
    self.retryStrategy = .exponentialBackoff(maxAttempts: count, initialDelay: delay, multiplier: 1.0)
    self.retryTolerance = tolerance
    self.retryErrorHandler = onError
    return self
  }

  /// Configure retry with a custom strategy and optional per-attempt error handling.
  ///
  /// Combines a custom ``RetryStrategy`` with per-error decision making via a closure
  /// returning ``RetryErrorAction``. Omit `onError` to retry on every error.
  ///
  /// - Parameters:
  ///   - strategy: The retry strategy controlling attempt count and delay calculation
  ///   - tolerance: Allowed deviation in each retry delay timer, passed to `Task.sleep`.
  ///     `nil` uses the system default (default: `nil`)
  ///   - onError: Optional closure returning a ``RetryErrorAction`` that controls whether
  ///     to retry this error. If `nil`, every error triggers the next attempt.
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example without per-error gating:
  /// ```swift
  /// dataSource { ... }
  ///     .retry(strategy: .exponentialBackoff(
  ///         maxAttempts: 5,
  ///         initialDelay: .seconds(1),
  ///         multiplier: 2.0
  ///     ))
  ///     .build()
  /// ```
  ///
  /// Example with per-error gating:
  /// ```swift
  /// dataSource { ... }
  ///     .retry(
  ///         strategy: .exponentialBackoff(maxAttempts: 5, initialDelay: .seconds(1)),
  ///         onError: { error in
  ///             if error is RateLimitError { return .retry }
  ///             return .stop
  ///         }
  ///     )
  ///     .build()
  /// ```
  public func retry(
    strategy: RetryStrategy,
    tolerance: Duration? = nil,
    onError: (@Sendable (Error) -> RetryErrorAction)? = nil
  ) -> Self {
    self.retryStrategy = strategy
    self.retryTolerance = tolerance
    self.retryErrorHandler = onError
    return self
  }

  /// Add a prerequisite check that must pass before fetching.
  ///
  /// Prerequisites are evaluated before each fetch. If any prerequisite fails,
  /// the fetch is aborted with a `PrerequisiteError`.
  ///
  /// - Parameter prerequisite: The prerequisite to check
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// struct NetworkAvailable: DataSourceRefreshPrerequisite {
  ///     func check() async -> Bool {
  ///         await NetworkMonitor.shared.isConnected
  ///     }
  /// }
  ///
  /// dataSource { ... }
  ///     .requires(NetworkAvailable())
  ///     .build()
  /// ```
  public func requires(_ prerequisite: DataSourceRefreshPrerequisite) -> Self {
    self.prerequisites.append(prerequisite)
    return self
  }

  /// Register an async closure to run before each fetch.
  ///
  /// The hook runs after all gate checks (prerequisites, TTL, throttle, debounce) but before
  /// the actual fetch. If the hook throws, the error is wrapped in a ``BeforeFetchError`` and
  /// passed to the `onError` handler — the fetch is then aborted and the error is re-thrown.
  ///
  /// Multiple `.beforeFetch()` calls execute concurrently. If any hook throws, the
  /// remaining hooks are cancelled and the fetch is aborted.
  ///
  /// - Parameter handler: Async throwing closure to run before the fetch
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource {
  ///     try await api.fetchProfile()
  /// } onError: { error in
  ///     if error is BeforeFetchError { return .keep }
  ///     return .clear
  /// }
  /// .beforeFetch { try await authSource.refresh() }
  /// .build()
  /// ```
  public func beforeFetch(_ handler: @escaping @Sendable () async throws -> Void) -> Self {
    beforeFetchHandlers.append(handler)
    return self
  }

  /// Refresh another data source before each fetch.
  ///
  /// Convenience overload that calls `refresh()` on the given data source before each fetch.
  /// If that refresh fails, the error is wrapped in a ``BeforeFetchError`` and the fetch is aborted.
  ///
  /// Because ``DataSource/refresh(clear:)`` deduplicates in-flight requests, calling this on a
  /// source that is already refreshing simply joins the existing task — no redundant work is done.
  ///
  /// - Parameter source: The data source to refresh before fetching
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource {
  ///     try await api.fetchProfile(token: authSource.value!)
  /// } onError: { _ in .keep }
  /// .beforeFetch(refresh: authSource)
  /// .build()
  /// ```
  public func beforeFetch<V: Sendable>(refresh source: DataSource<V>) -> Self {
    beforeFetchHandlers.append { _ = try await source.refresh() }
    return self
  }

  /// Register an async closure to run before each fetch, ignoring any errors it throws.
  ///
  /// The hook runs after all gate checks but before the actual fetch. Unlike ``beforeFetch(_:)``,
  /// any error thrown by the closure is silently discarded and the fetch proceeds regardless.
  ///
  /// Use this for best-effort preparation work where a failure should not block the fetch —
  /// for example, refreshing a non-critical cache or logging.
  ///
  /// - Parameter handler: Async throwing closure to attempt before the fetch
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource {
  ///     try await api.fetchFeed()
  /// } onError: { _ in .keep }
  /// .failableBeforeFetch { try await prefetchSource.refresh() }
  /// .build()
  /// ```
  public func failableBeforeFetch(_ handler: @escaping @Sendable () async throws -> Void) -> Self {
    beforeFetchHandlers.append { try? await handler() }
    return self
  }

  /// Filter out duplicate values using `Equatable` comparison.
  ///
  /// When configured, the data source only emits new values to the `values` stream
  /// if they differ from the previous value. This prevents unnecessary updates
  /// when the fetched data hasn't actually changed.
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// dataSource { ... }
  ///     .distinct()
  ///     .build()
  /// ```
  public func distinct() -> Self where Value: Equatable {
    self.distinctComparator = { $0 == $1 }
    return self
  }

  /// Filter out duplicate values using a custom comparator.
  ///
  /// Provides fine-grained control over what constitutes a "duplicate" value.
  /// The comparator should return `true` if the values are considered equal.
  ///
  /// - Parameter comparator: Closure that returns `true` if two values are equal
  ///
  /// - Returns: The builder for method chaining
  ///
  /// Example:
  /// ```swift
  /// struct User {
  ///     let id: Int
  ///     let name: String
  ///     let updatedAt: Date
  /// }
  ///
  /// dataSource { ... }
  ///     .distinct { oldUser, newUser in
  ///         oldUser.id == newUser.id && oldUser.name == newUser.name
  ///         // Ignore updatedAt in comparison
  ///     }
  ///     .build()
  /// ```
  public func distinct(_ comparator: @escaping @Sendable (Value, Value) -> Bool) -> Self {
    self.distinctComparator = comparator
    return self
  }

  /// Build the configured DataSource.
  ///
  /// Creates the final `DataSource` instance with all configured policies and behaviors.
  /// After building, the data source is ready to use but starts in an empty state.
  /// Call `refresh()` to fetch initial data.
  ///
  /// - Returns: A configured DataSource instance
  ///
  /// Example:
  /// ```swift
  /// let source = dataSource { ... }
  ///     .ttl(.seconds(300))
  ///     .retry(count: 3)
  ///     .build()
  ///
  /// // Now use the source
  /// let data = try await source.refresh()
  /// ```
  public func build() -> DataSource<Value> {
    let dataSource = DataSource(
      fetch: fetch,
      errorHandler: errorHandler,
      emptyValue: emptyValue,
      beforeFetchHandlers: beforeFetchHandlers,
      distinctComparator: distinctComparator,
      ttlDuration: ttlDuration,
      ttlClear: ttlClear,
      ttlTolerance: ttlTolerance,
      throttleDuration: throttleDuration,
      throttleLast: throttleLast,
      throttleTolerance: throttleTolerance,
      debounceDuration: debounceDuration,
      debounceTolerance: debounceTolerance,
      autoRefreshInterval: autoRefreshInterval,
      autoRefreshTolerance: autoRefreshTolerance,
      retryStrategy: retryStrategy,
      retryErrorHandler: retryErrorHandler,
      retryTolerance: retryTolerance,
      prerequisites: prerequisites,
      dependencyCoordinator: stateHolder
    )

    // Observe dependencies
    stateHolder.start(dataSource: dataSource)

    return dataSource
  }
}
