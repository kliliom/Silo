# Getting Started with Silo

Create your first data source, observe reactive streams, and understand the core lifecycle.

## Overview

Silo revolves around ``DataSource``, a `@MainActor` class that wraps an async fetch closure, maintains a cached value, and streams updates to all active subscribers. You create one with the ``DataSourceBuilder`` returned by the `dataSource()` function family, chain configuration methods, then call `.build()`.

```swift
@MainActor
let articlesSource = dataSource {
    try await api.fetchArticles()
} onError: { _ in .keep }
.ttl(.seconds(300))
.autoRefresh(.seconds(60))
.build()
```

## Adding Silo

In your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/kliliom/silo", from: "1.0.0")
],
targets: [
    .target(name: "MyApp", dependencies: ["Silo"])
]
```

## Choosing a Builder Overload

The `dataSource()` function has overloads that eliminate boilerplate for common value types:

| Value type | Overload to use | Empty state |
| --- | --- | --- |
| `User?`, `String?`, any `Optional` | `dataSource { ... } onError: { ... }` | `nil` |
| `[User]`, `[String]`, any `Array` | `dataSource { ... } onError: { ... }` | `[]` |
| `[String: Any]`, any `Dictionary` | `dataSource { ... } onError: { ... }` | `[:]` |
| Any other type | `dataSource { ... } onError: { ... } emptyValue: { ... }` | Your placeholder |

For `Optional`, `Array`, and `Dictionary` values the empty state is inferred automatically. For any other type you must supply an `emptyValue` closure that returns a placeholder:

```swift
// Optional — empty state is nil automatically
let profileSource = dataSource {
    try await api.fetchProfile() // User?
} onError: { _ in .keep }
.build()

// Array — empty state is [] automatically
let usersSource = dataSource {
    try await api.fetchUsers() // [User]
} onError: { _ in .keep }
.build()

// Custom type — must provide emptyValue
let statsSource = dataSource {
    try await api.fetchStats() // AppStats (non-optional struct)
} onError: { _ in .keep }
emptyValue: {
    AppStats.zero
}
.build()
```

> Important: `dataSource()` and all `DataSource` methods must be called on the `@MainActor`. Mark your containing type `@MainActor`, or call from within a `@MainActor` context such as a `Task { @MainActor in ... }`.

## Understanding the Empty Value

The empty value is what a `DataSource` holds before its first successful fetch, and what it reverts to after `clear()` is called or a TTL expiry clears it. It serves as the "zero state" for the type — a safe default that makes the source always usable without optionality at the call site.

Choose an empty value that is safe to display. A `User` might have `name: ""`, a `[Product]` is already `[]`, and a `Stats` might have all counters at `0`.

## Triggering a Fetch

Call ``DataSource/refresh(clear:)`` to load data. It returns the fetched value and throws on failure:

```swift
do {
    let articles = try await articlesSource.refresh()
    print("Loaded \(articles.count) articles")
} catch {
    print("Failed: \(error)")
}
```

`refresh()` is idempotent with respect to in-flight requests: if one is already running when a second caller invokes `refresh()`, both callers await the same underlying task. No duplicate network calls are made.

### Force-Refreshing Past the Cache

Pass `clear: true` to reset the cache and bypass any active TTL window:

```swift
// Pull-to-refresh — ignore cache, always fetch fresh
let articles = try await articlesSource.refresh(clear: true)
```

## Observing Values

Subscribe to ``DataSource/values`` to receive the current cached value immediately, then live updates whenever a `refresh()` completes:

```swift
for await articles in articlesSource.values {
    tableView.reloadData(with: articles)
}
```

The first element delivered to a new subscriber is always the current cached value — this might be the empty value if no fetch has happened yet, or the last successfully fetched value otherwise. This makes stream subscriptions self-bootstrapping: no separate "initial load" call is required.

> Tip: Use `.task { }` in SwiftUI views to subscribe and automatically cancel when the view disappears.

### Custom Buffering

The default stream buffers all values without limit. For fast-changing data, limit the buffer to avoid queuing stale values:

```swift
// Only keep the most recent update for a fast-changing source
for await price in stockSource.values(bufferingPolicy: .bufferingNewest(1)) {
    updatePriceLabel(price)
}
```

## Tracking Loading and Empty State

Subscribe to ``DataSource/state`` to drive loading indicators and empty-state views:

```swift
for await state in articlesSource.state {
    loadingView.isHidden = !state.isRefreshing
    emptyView.isHidden = !state.isEmpty
}
```

``DataSourceState`` has two boolean properties:

| Property | `true` when... | `false` when... |
| --- | --- | --- |
| `isRefreshing` | A fetch is in progress | No fetch is running |
| `isEmpty` | The current value is the empty value | Data has been successfully fetched |

Possible state combinations:

| `isRefreshing` | `isEmpty` | Meaning |
| --- | --- | --- |
| `false` | `true` | Initial state — nothing fetched yet |
| `true` | `false` | Refreshing while showing existing data |
| `true` | `true` | First fetch in progress (or refreshing after clear) |
| `false` | `false` | Data loaded and idle |

## Observing Value and State Together

Use ``DataSource/valueWithState`` to get a ``DataSourceValueWithState`` snapshot on every change — value or state. This is the most convenient stream for driving a SwiftUI view:

```swift
for await snapshot in articlesSource.valueWithState {
    loadingSpinner.isVisible = snapshot.state.isRefreshing
    updateList(snapshot.value)
}
```

> Note: Subscribing to `valueWithState` counts toward the active subscriber count for auto-refresh purposes, just like `values` does.

## Clearing Cached Data

``DataSource/clear()`` resets the source to its empty state immediately:

```swift
func signOut() async {
    profileSource.clear()
    articlesSource.clear()
}
```

Clearing:
- Sets the cached value to the result of `emptyValue()`
- Emits the empty value to all `values` subscribers
- Sets `isEmpty` to `true`
- Cancels active TTL, throttle, and debounce timers
- Does **not** cancel an in-flight fetch task

## Cancelling In-Flight Fetches

``DataSource/cancelRefresh(clear:)`` cancels any running fetch task:

```swift
// Cancel without clearing cache
articlesSource.cancelRefresh()

// Cancel and wipe the cache
articlesSource.cancelRefresh(clear: true)
```

Any `refresh()` callers waiting on the cancelled task receive a `CancellationError`.

## Data Source Lifecycle

```
Created           isEmpty: true   isRefreshing: false
   ↓
refresh() called  isEmpty: true   isRefreshing: true   (first fetch)
   ↓
Fetch succeeds    isEmpty: false  isRefreshing: false  → value emitted
   ↓
refresh() called  isEmpty: false  isRefreshing: true   (subsequent fetch)
   ↓
Fetch succeeds    isEmpty: false  isRefreshing: false  → value emitted (if changed)
   ↓
clear() / TTL     isEmpty: true   isRefreshing: false  → empty value emitted
```

## Next Steps

- Add TTL caching and auto-expiry: <doc:Caching>
- Configure retry and error handling: <doc:ErrorHandling>
- Set up throttle, debounce, and periodic auto-refresh: <doc:TimeControls>
- React to upstream state changes with dependencies: <doc:Dependencies>
- Gate fetches on conditions like network availability: <doc:Prerequisites>
- Walk through building a complete app: <doc:BuildingAD6DiceRoller>
