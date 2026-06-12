@MainActor
protocol DependencyCoordinating: Sendable {
  func start<T: Sendable>(dataSource: DataSource<T>)
  func stopObserving()
  func checkPendingLazyRefresh<T: Sendable>(dataSource: DataSource<T>) async
  func markRefreshCompleted()
}

final class DependencyCoordinator<each Dependency: Sendable>: DependencyCoordinating {
  // swift-format-ignore
  typealias ValueStore = (repeat Optional<each Dependency>)
  typealias Value = (repeat each Dependency)

  var dependency: (repeat DataSourceDependency<each Dependency>)
  var valueStore: ValueStore
  var hasPendingLazyRefresh: Bool = false
  var observerTasks: [Task<Void, Never>] = []

  init(dependency: repeat DataSourceDependency<each Dependency>) {
    self.dependency = (repeat each dependency)
    valueStore = (repeat (Optional<each Dependency>).none)
  }

  deinit {
    // Safety net for when the owning DataSource is deallocated without terminate()
    for task in observerTasks { task.cancel() }
  }

  func updateValue<T: Sendable>(
    at targetIndex: Int,
    with newValue: T
  ) {
    var index = 0
    valueStore =
      (repeat select(currentValue: each valueStore, newValue: newValue, currentIndex: &index, newIndex: targetIndex))
  }

  var value: Value? {
    do {
      return try (repeat (each valueStore).require())
    } catch {
      return nil
    }
  }

  func start<T: Sendable>(dataSource: DataSource<T>) {
    var index = 0
    repeat
      ({
        defer { index += 1 }
        startObserver(dataSource: dataSource, source: each dependency, at: index)
      }())
  }

  func stopObserving() {
    for task in observerTasks { task.cancel() }
    observerTasks.removeAll()
  }

  func checkPendingLazyRefresh<T: Sendable>(dataSource: DataSource<T>) async {
    guard hasPendingLazyRefresh, value != nil else { return }
    hasPendingLazyRefresh = false
    _ = try? await dataSource.refresh()
  }

  func markRefreshCompleted() {
    hasPendingLazyRefresh = false
  }

  private func startObserver<T: Sendable, D: Sendable>(
    dataSource: DataSource<T>,
    source: DataSourceDependency<D>,
    at index: Int
  ) {
    // Weak captures: a long-lived dependency stream must not keep the data source
    // (or this coordinator) alive. Strong references are only held per iteration.
    let task = Task { @MainActor [weak self, weak dataSource] in
      for await streamValue in source.stream {
        guard let self = self, let dataSource = dataSource else { return }
        self.updateValue(at: index, with: streamValue)

        // Check if we should refresh based on policy
        let shouldRefresh = dataSource.shouldRefreshForPolicy(source.policy)

        if shouldRefresh, self.value != nil {
          _ = try? await dataSource.refresh(clear: source.clear)
        } else if source.policy == .lazy, !dataSource.hasActiveSubscribers {
          // Lazy policy without subscribers - mark for refresh when subscriber arrives
          self.hasPendingLazyRefresh = true
        }
      }
    }
    observerTasks.append(task)
  }
}

// MARK: - Utilities

private func select<CurrentValue: Sendable, NewValue: Sendable>(
  currentValue: CurrentValue,
  newValue: NewValue,
  currentIndex: inout Int,
  newIndex: Int
) -> CurrentValue {
  defer { currentIndex += 1 }
  if newIndex == currentIndex {
    precondition(
      newValue as? CurrentValue != nil,
      "Type mismatch when updating dependency at index \(newIndex): expected \(CurrentValue.self), got \(NewValue.self)"
    )
    return newValue as! CurrentValue
  } else {
    return currentValue
  }
}

// MARK: - Optional Extensions

extension Optional {
  private struct NotSetError: Error {}

  func require() throws -> Wrapped {
    switch self {
    case .none:
      throw NotSetError()
    case .some(let wrapped):
      wrapped
    }
  }
}

// MARK: - DataSource Extensions

extension DataSource {
  fileprivate var hasActiveSubscribers: Bool {
    activeSubscriberCount > 0
  }

  fileprivate func shouldRefreshForPolicy(_ policy: RefreshPolicy) -> Bool {
    switch policy {
    case .eager:
      return true
    case .lazy:
      return hasActiveSubscribers
    case .manual:
      return false
    }
  }
}
