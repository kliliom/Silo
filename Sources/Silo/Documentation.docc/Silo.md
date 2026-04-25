# ``Silo``

Reactive async data management for Swift 6 — fetch, cache, and stream data with zero dependencies.

## Overview

Silo gives you a single type, ``DataSource``, that handles the full lifecycle of async data: fetching, caching, state tracking, and broadcasting updates to all observers through a reactive `AsyncStream` interface.

Every configuration decision — TTL, retries, throttling, dependencies — is expressed through a fluent builder, so a data layer reads like a specification:

```swift
let postsSource = dataSource(selectedCategory.dependency(.lazy, clear: true)) { category in
    try await api.fetchPosts(in: category)
} onError: { error in
    error is URLError ? .keep : .clear
}
.ttl(.seconds(120))
.autoRefresh(.seconds(30))
.retry(strategy: .exponentialBackoff(maxAttempts: 3, initialDelay: .seconds(1)))
.distinct()
.build()
```

Silo is built entirely on Swift's structured concurrency model. All public API is `@MainActor`, all types conform to `Sendable`, and thread safety is compiler-enforced — not lock-based.

### What Silo Handles For You

- **Request deduplication** — concurrent `refresh()` calls share one in-flight fetch task
- **Cache coherence** — TTL windows prevent redundant fetches; expiry clears data automatically when configured
- **Backpressure** — configurable `AsyncStream` buffering policies for slow consumers
- **Subscriber lifecycle** — auto-refresh starts when the first subscriber appears; stops when the last one terminates
- **Dependency freshness** — re-fetch automatically when upstream async streams emit new values; `.lazy` policy defers the fetch until someone is watching

## Topics

### Getting Started

- <doc:GettingStarted>
- <doc:BuildingAD6DiceRoller>
- <doc:ReactiveDependencies>

### Core Types

- ``DataSource``
- ``DataSourceBuilder``

### Creating Data Sources

- ``dataSource(_:fetch:onError:emptyValue:)``

### Observing State

- ``DataSourceState``
- ``DataSourceValueWithState``

### Error Handling and Retry

- ``FetchErrorAction``
- ``RetryErrorAction``
- ``RetryStrategy``

### Reactive Dependencies

- ``DataSourceDependency``
- ``RefreshPolicy``

### Conditional Fetching

- ``DataSourceRefreshPrerequisite``
- ``PrerequisiteError``

### Guides

- <doc:Caching>
- <doc:ErrorHandling>
- <doc:TimeControls>
- <doc:Dependencies>
- <doc:Prerequisites>
