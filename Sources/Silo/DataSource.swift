import Foundation

/// Manages asynchronous data fetching, caching, and reactive streaming for a single value type.
///
/// Create a data source with the ``dataSource(_:fetch:onError:emptyValue:)`` function, configure it
/// with the builder, then subscribe to ``values``, ``state``, or ``valueWithState``.
///
/// ## Lifecycle
///
/// 1. Starts empty — `isEmpty: true`, cached value is `emptyValue()`
/// 2. Call `refresh()` to fetch. State becomes `isRefreshing: true`
/// 3. On success, cache is updated, `isEmpty` becomes `false`, value is emitted to all subscribers
/// 4. On error, `onError` decides whether to keep or clear the cache; error is rethrown
/// 5. TTL expiry or `clear()` resets to empty
///
/// ## Basic Usage
///
/// ```swift
/// let userSource = dataSource {
///     try await API.getUser()
/// } onError: { _ in
///     .keep
/// } emptyValue: {
///     User.placeholder
/// }
/// .build()
///
/// let user = try await userSource.refresh()
///
/// for await user in userSource.values {
///     updateUI(with: user)
/// }
/// ```
///
/// ## Time-Based Policies
///
/// ```swift
/// dataSource { ... }
///     .ttl(.seconds(300))           // skip fetch if data is < 5 min old
///     .throttle(.seconds(1))        // at most one fetch per second
///     .debounce(.milliseconds(300)) // wait 300ms of quiet before fetching
///     .autoRefresh(.seconds(60))    // re-fetch every minute while observed
///     .build()
/// ```
///
/// ## Retry
///
/// ```swift
/// dataSource { ... }
///     .retry(strategy: .exponentialBackoff(maxAttempts: 3, initialDelay: .seconds(1))) { error in
///         error is URLError ? .retry : .stop
///     }
///     .build()
/// ```
@MainActor
public final class DataSource<Value: Sendable>: Sendable {
  // MARK: - State

  let fetch: @Sendable () async throws -> Value
  let errorHandler: @Sendable (Error) async -> FetchErrorAction
  let emptyValue: @Sendable () -> Value
  let beforeFetchHandlers: [@Sendable () async throws -> Void]
  let distinctComparator: (@Sendable (Value, Value) -> Bool)?

  let ttlDuration: Duration?
  let ttlClear: Bool
  let ttlTolerance: Duration?
  let throttleDuration: Duration?
  let throttleLast: Bool
  let throttleTolerance: Duration?
  let debounceDuration: Duration?
  let debounceTolerance: Duration?
  let autoRefreshInterval: Duration?
  let autoRefreshTolerance: Duration?
  let retryStrategy: RetryStrategy?
  let retryErrorHandler: (@Sendable (Error) async -> RetryErrorAction)?
  let retryTolerance: Duration?
  let prerequisites: [DataSourceRefreshPrerequisite]
  let dependencyCoordinator: (any DependencyCoordinating)?

  // Mutable state
  var cachedValue: Value
  var isEmpty: Bool = true
  var isRefreshing: Bool = false

  // Stream management
  var valueContinuations: [UUID: AsyncStream<Value>.Continuation] = [:]
  var stateContinuations: [UUID: AsyncStream<DataSourceState>.Continuation] = [:]
  var valueWithStateContinuations: [UUID: AsyncStream<DataSourceValueWithState<Value>>.Continuation] = [:]
  internal var activeSubscriberCount: Int = 0

  // In-flight request tracking
  var currentFetchTask: Task<Value, Error>?

  // Timer state
  var ttlExpiryTime: Date?
  var throttleExpiryTime: Date?
  var ttlExpiryTask: Task<Void, Never>?
  var debounceTask: Task<Void, Never>?
  var debounceCounter: Int = 0
  var autoRefreshTask: Task<Void, Never>?
  var autoRefreshPaused: Bool = false

  // MARK: - Initialization

  init(
    fetch: @escaping @Sendable () async throws -> Value,
    errorHandler: @escaping @Sendable (Error) async -> FetchErrorAction,
    emptyValue: @escaping @Sendable () -> Value,
    beforeFetchHandlers: [@Sendable () async throws -> Void] = [],
    distinctComparator: (@Sendable (Value, Value) -> Bool)?,
    ttlDuration: Duration?,
    ttlClear: Bool,
    ttlTolerance: Duration?,
    throttleDuration: Duration?,
    throttleLast: Bool,
    throttleTolerance: Duration?,
    debounceDuration: Duration?,
    debounceTolerance: Duration?,
    autoRefreshInterval: Duration?,
    autoRefreshTolerance: Duration?,
    retryStrategy: RetryStrategy?,
    retryErrorHandler: (@Sendable (Error) async -> RetryErrorAction)?,
    retryTolerance: Duration?,
    prerequisites: [DataSourceRefreshPrerequisite],
    dependencyCoordinator: (any DependencyCoordinating)? = nil
  ) {
    self.fetch = fetch
    self.errorHandler = errorHandler
    self.emptyValue = emptyValue
    self.beforeFetchHandlers = beforeFetchHandlers
    self.distinctComparator = distinctComparator
    self.ttlDuration = ttlDuration
    self.ttlClear = ttlClear
    self.ttlTolerance = ttlTolerance
    self.throttleDuration = throttleDuration
    self.throttleLast = throttleLast
    self.throttleTolerance = throttleTolerance
    self.debounceDuration = debounceDuration
    self.debounceTolerance = debounceTolerance
    self.autoRefreshInterval = autoRefreshInterval
    self.autoRefreshTolerance = autoRefreshTolerance
    self.retryStrategy = retryStrategy
    self.retryErrorHandler = retryErrorHandler
    self.retryTolerance = retryTolerance
    self.prerequisites = prerequisites
    self.dependencyCoordinator = dependencyCoordinator

    // Initialize with empty value
    self.cachedValue = emptyValue()
  }

  deinit {
    // NOTE: same as `terminate()`
    currentFetchTask?.cancel()
    currentFetchTask = nil
    ttlExpiryTask?.cancel()
    ttlExpiryTask = nil
    ttlExpiryTime = nil
    debounceTask?.cancel()
    debounceTask = nil
    debounceCounter = 0
    autoRefreshTask?.cancel()
    autoRefreshTask = nil
    for continuation in valueContinuations.values { continuation.finish() }
    valueContinuations.removeAll()
    for continuation in stateContinuations.values { continuation.finish() }
    stateContinuations.removeAll()
    for continuation in valueWithStateContinuations.values { continuation.finish() }
    valueWithStateContinuations.removeAll()
    activeSubscriberCount = 0
    throttleExpiryTime = nil
    autoRefreshPaused = false
    isRefreshing = false
  }

  // MARK: - Public API

  /// Infinite stream of values, replaying the current cached value immediately on subscription.
  ///
  /// Emits on each successful `refresh()`, `clear()`, and TTL-triggered clear (`ttlClear: true`).
  /// With `.distinct()` configured, only emits when the value actually changes.
  ///
  /// Drives the auto-refresh lifecycle: the timer starts on first subscriber and stops when
  /// the last one terminates.
  ///
  /// ```swift
  /// for await user in userSource.values {
  ///     updateUI(with: user)
  /// }
  /// ```
  public var values: AsyncStream<Value> {
    values()
  }

  /// The ``values`` stream with a custom buffering policy.
  ///
  /// Identical to the `values` property. Use this when the consumer may fall behind and you
  /// want to control whether intermediate values are dropped or queued.
  ///
  /// - Parameter limit: Buffering policy when the consumer is slow. Defaults to `.unbounded`.
  /// - Returns: An async stream of values.
  ///
  /// ```swift
  /// // Drop intermediate updates — UI only needs the latest value
  /// for await user in userSource.values(bufferingPolicy: .bufferingNewest(1)) {
  ///     updateUI(with: user)
  /// }
  /// ```
  public func values(
    bufferingPolicy limit: AsyncStream<Value>.Continuation.BufferingPolicy = .unbounded
  ) -> AsyncStream<Value> {
    let id = UUID()
    return AsyncStream(bufferingPolicy: limit) { continuation in
      // Emit current value immediately
      continuation.yield(self.cachedValue)

      // Store continuation for future updates
      self.valueContinuations[id] = continuation

      // Track subscriber count
      self.activeSubscriberCount += 1
      if self.activeSubscriberCount == 1 {
        // First subscriber arrived - check for pending lazy refresh
        if let coordinator = self.dependencyCoordinator {
          Task { @MainActor in
            await coordinator.checkPendingLazyRefresh(dataSource: self)
          }
        }

        // Start auto-refresh if configured
        if self.autoRefreshInterval != nil {
          self.startAutoRefresh(immediate: true)
        }
      }

      continuation.onTermination = { [weak self] _ in
        Task { @MainActor [weak self] in
          self?.valueContinuations.removeValue(forKey: id)
          if let self = self {
            self.activeSubscriberCount -= 1
            if self.activeSubscriberCount == 0 && self.autoRefreshInterval != nil {
              self.stopAutoRefresh()
            }
          }
        }
      }
    }
  }

  /// Infinite stream of ``DataSourceState`` updates, replaying the current state on subscription.
  ///
  /// Emits whenever `isRefreshing` or `isEmpty` changes. Use this to drive loading indicators
  /// and empty-state views independently of the data value.
  ///
  /// ```swift
  /// for await state in userSource.state {
  ///     loadingIndicator.isVisible = state.isRefreshing
  ///     emptyView.isVisible = state.isEmpty && !state.isRefreshing
  /// }
  /// ```
  public var state: AsyncStream<DataSourceState> {
    state()
  }

  /// The ``state`` stream with a custom buffering policy.
  ///
  /// - Parameter limit: Buffering policy when the consumer is slow. Defaults to `.unbounded`.
  /// - Returns: An async stream of ``DataSourceState`` updates.
  ///
  /// ```swift
  /// for await state in userSource.state(bufferingPolicy: .bufferingNewest(1)) {
  ///     loadingIndicator.isVisible = state.isRefreshing
  /// }
  /// ```
  public func state(
    bufferingPolicy limit: AsyncStream<DataSourceState>.Continuation.BufferingPolicy = .unbounded
  ) -> AsyncStream<DataSourceState> {
    let id = UUID()
    return AsyncStream(bufferingPolicy: limit) { continuation in
      // Emit current state immediately
      continuation.yield(DataSourceState(isRefreshing: self.isRefreshing, isEmpty: self.isEmpty))

      // Store continuation for future updates
      self.stateContinuations[id] = continuation

      continuation.onTermination = { [weak self] _ in
        Task { @MainActor [weak self] in
          self?.stateContinuations.removeValue(forKey: id)
        }
      }
    }
  }

  /// Infinite stream of ``DataSourceValueWithState`` snapshots, replaying the current snapshot on subscription.
  ///
  /// Emits on every value change or state change. Use this instead of subscribing to `values` and
  /// `state` separately when you need both in the same handler.
  ///
  /// Like `values`, counts toward the active subscriber count and drives the auto-refresh lifecycle.
  ///
  /// ```swift
  /// for await snapshot in userSource.valueWithState {
  ///     loadingIndicator.isVisible = snapshot.state.isRefreshing
  ///     nameLabel.text = snapshot.value.name
  /// }
  /// ```
  public var valueWithState: AsyncStream<DataSourceValueWithState<Value>> {
    valueWithState()
  }

  /// The ``valueWithState`` stream with a custom buffering policy.
  ///
  /// - Parameter limit: Buffering policy when the consumer is slow. Defaults to `.unbounded`.
  /// - Returns: An async stream of ``DataSourceValueWithState`` snapshots.
  ///
  /// ```swift
  /// for await snapshot in userSource.valueWithState(bufferingPolicy: .bufferingNewest(1)) {
  ///     updateUI(snapshot.value, isLoading: snapshot.state.isRefreshing)
  /// }
  /// ```
  public func valueWithState(
    bufferingPolicy limit: AsyncStream<DataSourceValueWithState<Value>>.Continuation.BufferingPolicy =
      .unbounded
  ) -> AsyncStream<DataSourceValueWithState<Value>> {
    let id = UUID()
    return AsyncStream(bufferingPolicy: limit) { continuation in
      // Emit current value and state immediately
      continuation.yield(
        DataSourceValueWithState(
          value: self.cachedValue,
          state: DataSourceState(isRefreshing: self.isRefreshing, isEmpty: self.isEmpty)
        )
      )

      self.valueWithStateContinuations[id] = continuation

      self.activeSubscriberCount += 1
      if self.activeSubscriberCount == 1 {
        if let coordinator = self.dependencyCoordinator {
          Task { @MainActor in
            await coordinator.checkPendingLazyRefresh(dataSource: self)
          }
        }
        if self.autoRefreshInterval != nil {
          self.startAutoRefresh(immediate: true)
        }
      }

      continuation.onTermination = { [weak self] _ in
        Task { @MainActor [weak self] in
          self?.valueWithStateContinuations.removeValue(forKey: id)
          if let self = self {
            self.activeSubscriberCount -= 1
            if self.activeSubscriberCount == 0 && self.autoRefreshInterval != nil {
              self.stopAutoRefresh()
            }
          }
        }
      }
    }
  }

  /// Fetches new data, updating the cache and notifying all stream subscribers on success.
  ///
  /// Policies are applied in this order:
  /// 1. **TTL**: Returns the cached value immediately if still within its TTL window — no further work done.
  /// 2. **Debounce**: Waits for calls to settle; throws `CancellationError` if superseded.
  /// 3. **Throttle**: Drops (returns cached) or waits then continues, depending on `last:`.
  /// 4. **Prerequisites**: Checked once per settled fetch attempt; throws ``PrerequisiteError`` if any fail.
  /// 5. **Deduplication**: Concurrent callers join the existing in-flight task.
  /// 6. **beforeFetch hooks**: Run concurrently; errors are wrapped in ``BeforeFetchError`` and abort the fetch.
  /// 7. **Fetch + Retry**: Calls the fetch closure; retries per the configured ``RetryStrategy``.
  ///
  /// On success, the cache is updated, `isEmpty` becomes `false`, and the new value is emitted
  /// to all `values` and `valueWithState` subscribers (subject to `.distinct()`).
  ///
  /// On error, `onError` decides whether to keep or clear the cache. The error is always rethrown.
  ///
  /// - Parameter clear: If `true`, clears the cache before fetching, bypassing TTL. Default: `false`.
  /// - Returns: The fetched value.
  /// - Throws: Rethrows errors from prerequisites, `beforeFetch` hooks, or the fetch closure.
  ///
  /// ```swift
  /// let user = try await userSource.refresh()
  ///
  /// // Bypass TTL and force a fresh fetch
  /// let user = try await userSource.refresh(clear: true)
  /// ```
  @discardableResult
  public func refresh(clear: Bool = false) async throws -> Value {
    // clear: resets cache before TTL check so the cleared state is observed
    if clear {
      self.clear()
    }

    // TTL: return immediately if data is fresh — no fetch, no waiting, no prerequisite checks
    if ttlDuration != nil, !isEmpty {
      if let expiryTime = ttlExpiryTime, Date() < expiryTime {
        return cachedValue
      }
    }

    // Debounce: coalesce rapid calls before proceeding; superseded calls throw CancellationError
    if let debounceDuration = debounceDuration {
      debounceTask?.cancel()
      debounceCounter += 1
      let currentCounter = debounceCounter

      debounceTask = Task {
        try? await Task.sleep(for: debounceDuration, tolerance: self.debounceTolerance)
      }
      await debounceTask?.value

      guard currentCounter == debounceCounter else {
        throw CancellationError()
      }
    }

    // Throttle: drop or queue; only runs when a fetch is actually needed
    if let throttleDuration = throttleDuration {
      if let expiryTime = throttleExpiryTime, Date() < expiryTime {
        if throttleLast {
          let waitDuration = expiryTime.timeIntervalSinceNow
          if waitDuration > 0 {
            try await Task.sleep(for: .seconds(waitDuration), tolerance: throttleTolerance)
          }
        } else {
          return cachedValue
        }
      }
      throttleExpiryTime = Date().addingTimeInterval(throttleDuration.timeInterval)
    }

    // Prerequisites: checked once per settled fetch attempt, after debounce/throttle
    for prerequisite in prerequisites {
      let passed = await prerequisite.check()
      if !passed {
        throw PrerequisiteError(message: "Prerequisite check failed")
      }
    }

    return try await performFetch()
  }

  /// Resets the data source to its empty state.
  ///
  /// Replaces the cached value with `emptyValue()`, emits it to all subscribers,
  /// sets `isEmpty: true`, and cancels all active timers (TTL, throttle, debounce).
  /// Does not cancel an in-flight fetch — use `cancelRefresh(clear: true)` for that.
  ///
  /// ```swift
  /// userSource.clear() // e.g. on logout
  /// ```
  public func clear() {
    cachedValue = emptyValue()
    isEmpty = true
    emitValue(cachedValue)
    emitState()
    emitValueWithState()

    // Reset timers
    resetTimers()
  }

  /// Cancel any in-flight fetch operation.
  ///
  /// Immediately cancels the current fetch task if one is in progress:
  /// - Cancels the fetch task
  /// - Updates state to `isRefreshing: false`
  /// - Optionally clears cached data
  ///
  /// Any pending `refresh()` calls waiting on the cancelled task will receive
  /// a cancellation error.
  ///
  /// - Parameter clear: If `true`, also clears the cached value by calling `clear()`
  ///
  /// Example:
  /// ```swift
  /// // Cancel ongoing fetch when view disappears
  /// viewDidDisappear {
  ///     userSource.cancelRefresh()
  /// }
  ///
  /// // Cancel and clear data when user logs out
  /// logout {
  ///     userSource.cancelRefresh(clear: true)
  /// }
  /// ```
  public func cancelRefresh(clear: Bool = false) {
    currentFetchTask?.cancel()
    currentFetchTask = nil

    if clear {
      self.clear()
    }

    isRefreshing = false
    emitState()
    emitValueWithState()
  }

  /// Stop auto-refresh timer.
  ///
  /// Pauses the auto-refresh timer if one is configured. Has no effect if auto-refresh
  /// is not configured via `.autoRefresh()`.
  ///
  /// The timer remains stopped until `resumeAutoRefresh()` or `restartAutoRefresh()` is called.
  /// Note that auto-refresh automatically stops when all subscribers to `values` terminate.
  ///
  /// Example:
  /// ```swift
  /// // Pause auto-refresh when app goes to background
  /// appDidEnterBackground {
  ///     userSource.stopAutoRefresh()
  /// }
  /// ```
  public func stopAutoRefresh() {
    guard autoRefreshInterval != nil else { return }
    autoRefreshPaused = true
    autoRefreshTask?.cancel()
    autoRefreshTask = nil
  }

  /// Resume auto-refresh timer.
  ///
  /// Resumes the auto-refresh timer if it was previously stopped with `stopAutoRefresh()`.
  /// Has no effect if:
  /// - Auto-refresh is not configured via `.autoRefresh()`
  /// - Auto-refresh is already running
  /// - There are no active subscribers to `values`
  ///
  /// When resumed, triggers an immediate refresh before restarting the periodic timer.
  /// This ensures data does not stay stale for a full interval after the app returns to
  /// the foreground. To reset the timer without an immediate refresh, use `restartAutoRefresh(immediate: false)`.
  ///
  /// Example:
  /// ```swift
  /// // Resume auto-refresh when app returns to foreground
  /// appDidBecomeActive {
  ///     userSource.resumeAutoRefresh()
  /// }
  /// ```
  public func resumeAutoRefresh() {
    guard autoRefreshInterval != nil, autoRefreshPaused else { return }
    autoRefreshPaused = false
    if activeSubscriberCount > 0 {
      startAutoRefresh(immediate: true)
    }
  }

  /// Restart auto-refresh timer.
  ///
  /// Stops the current auto-refresh timer (if running) and starts a new one from zero.
  /// Has no effect if auto-refresh is not configured via `.autoRefresh()`.
  ///
  /// This is useful when you want to reset the refresh cadence, such as after a manual
  /// refresh or when data dependencies change.
  ///
  /// - Parameter immediate: If `true`, triggers an immediate refresh before restarting the timer
  ///
  /// Example:
  /// ```swift
  /// // Restart timer after manual refresh
  /// await userSource.restartAutoRefresh(immediate: false)
  ///
  /// // Force refresh and reset timer
  /// await userSource.restartAutoRefresh(immediate: true)
  /// ```
  public func restartAutoRefresh(immediate: Bool = false) async {
    guard autoRefreshInterval != nil else { return }

    stopAutoRefresh()
    autoRefreshPaused = false

    if immediate {
      _ = try? await refresh()
    }

    if activeSubscriberCount > 0 {
      startAutoRefresh(immediate: false)
    }
  }

  /// Cancels all in-flight tasks and finishes all active streams.
  ///
  /// After calling this, all `values`, `state`, and `valueWithState` streams finish,
  /// signalling end-of-sequence to their subscribers. Any in-flight `refresh()` call
  /// is cancelled. Call this for eager resource cleanup; `deinit` calls it automatically.
  ///
  /// ```swift
  /// dataSource.terminate()
  /// ```
  public func terminate() {
    currentFetchTask?.cancel()
    currentFetchTask = nil
    ttlExpiryTask?.cancel()
    ttlExpiryTask = nil
    ttlExpiryTime = nil
    debounceTask?.cancel()
    debounceTask = nil
    debounceCounter = 0
    autoRefreshTask?.cancel()
    autoRefreshTask = nil
    for continuation in valueContinuations.values { continuation.finish() }
    valueContinuations.removeAll()
    for continuation in stateContinuations.values { continuation.finish() }
    stateContinuations.removeAll()
    for continuation in valueWithStateContinuations.values { continuation.finish() }
    valueWithStateContinuations.removeAll()
    activeSubscriberCount = 0
    throttleExpiryTime = nil
    autoRefreshPaused = false
    isRefreshing = false
  }

  // MARK: - Methods

  func performFetch() async throws -> Value {
    // Deduplicate in-flight requests
    if let existingTask = currentFetchTask {
      return try await existingTask.value
    }

    isRefreshing = true
    isEmpty = false
    emitState()
    emitValueWithState()

    let fetchTask = Task<Value, Error> {
      do {
        if !beforeFetchHandlers.isEmpty {
          try await withThrowingTaskGroup(of: Void.self) { group in
            for handler in beforeFetchHandlers {
              group.addTask { try await handler() }
            }
            do {
              try await group.waitForAll()
            } catch {
              group.cancelAll()
              throw BeforeFetchError(underlyingError: error)
            }
          }
        }

        let newValue = try await fetchWithRetry()

        // Check if value changed (if distinct is enabled)
        let shouldEmit: Bool
        if let comparator = distinctComparator {
          shouldEmit = !comparator(cachedValue, newValue)
        } else {
          shouldEmit = true
        }

        cachedValue = newValue
        isEmpty = false

        if shouldEmit {
          emitValue(newValue)
          emitValueWithState()
        }

        // Start TTL timer
        startTTLTimer()

        // Reset auto-refresh
        if autoRefreshInterval != nil {
          startAutoRefresh(immediate: false)
        }

        // Mark dependency refresh as completed
        dependencyCoordinator?.markRefreshCompleted()

        return newValue
      } catch {
        let action = await errorHandler(error)
        switch action {
        case .keep:
          break
        case .clear:
          cachedValue = emptyValue()
          isEmpty = true
          emitValue(cachedValue)
          emitValueWithState()
        }
        throw error
      }
    }

    currentFetchTask = fetchTask

    do {
      let value = try await fetchTask.value
      currentFetchTask = nil
      isRefreshing = false
      emitState()
      emitValueWithState()
      return value
    } catch {
      currentFetchTask = nil
      isRefreshing = false
      emitState()
      emitValueWithState()
      throw error
    }
  }

  func fetchWithRetry() async throws -> Value {
    guard let strategy = retryStrategy else {
      return try await fetch()
    }

    var lastError: Error?
    for attempt in 1...strategy.maxAttempts {
      do {
        return try await fetch()
      } catch {
        lastError = error

        // Check retry error handler
        if let handler = retryErrorHandler {
          let action = await handler(error)
          switch action {
          case .retry:
            if attempt < strategy.maxAttempts {
              let delay = strategy.delay(for: attempt)
              try await Task.sleep(for: delay, tolerance: retryTolerance)
              continue
            }
          case .stop:
            throw error
          }
        }
        // No handler or handler returned .retry on last attempt

        // No handler or returned .retry
        if attempt < strategy.maxAttempts {
          let delay = strategy.delay(for: attempt)
          try await Task.sleep(for: delay, tolerance: retryTolerance)
        } else {
          throw error
        }
      }
    }

    throw lastError ?? NSError(domain: "DataSource", code: -1)
  }

  func emitValue(_ value: Value) {
    for continuation in valueContinuations.values {
      continuation.yield(value)
    }
  }

  func emitState() {
    let state = DataSourceState(isRefreshing: isRefreshing, isEmpty: isEmpty)
    for continuation in stateContinuations.values {
      continuation.yield(state)
    }
  }

  func emitValueWithState() {
    let snapshot = DataSourceValueWithState(
      value: cachedValue,
      state: DataSourceState(isRefreshing: isRefreshing, isEmpty: isEmpty)
    )
    for continuation in valueWithStateContinuations.values {
      continuation.yield(snapshot)
    }
  }

  func startTTLTimer() {
    guard let ttlDuration = ttlDuration else { return }

    let expiryTime = Date().addingTimeInterval(ttlDuration.timeInterval)
    ttlExpiryTime = expiryTime

    ttlExpiryTask?.cancel()
    ttlExpiryTask = Task { [weak self] in
      try? await Task.sleep(for: ttlDuration, tolerance: self?.ttlTolerance)
      guard !Task.isCancelled, let self = self else { return }

      if self.ttlClear {
        self.clear()
      }
    }
  }

  func startAutoRefresh(immediate: Bool) {
    guard let interval = autoRefreshInterval,
      !autoRefreshPaused,
      autoRefreshTask == nil
    else { return }

    autoRefreshTask = Task { [weak self] in
      if immediate {
        guard !Task.isCancelled, let self else { return }
        _ = try? await self.refresh()
      }
      while !Task.isCancelled {
        guard let self else { return }
        try? await Task.sleep(for: interval, tolerance: self.autoRefreshTolerance)
        if Task.isCancelled { return }
        _ = try? await self.refresh()
      }
    }
  }

  func resetTimers() {
    ttlExpiryTask?.cancel()
    ttlExpiryTask = nil
    ttlExpiryTime = nil

    throttleExpiryTime = nil

    debounceTask?.cancel()
    debounceTask = nil
    debounceCounter = 0

    if autoRefreshInterval != nil {
      startAutoRefresh(immediate: false)
    }
  }
}
