# Dependencies and Reactive Fetching

Automatically re-fetch data whenever upstream async stream values change.

## Overview

A ``DataSource`` can observe one or more upstream `AsyncStream` values called *dependencies*. When a dependency emits a new value, the data source re-fetches — passing the latest dependency values directly into the fetch closure.

This is the idiomatic way to model data that derives from other changing state:

```swift
// An upstream stream of the currently selected user ID
let selectedUserID: AsyncStream<Int>

// Automatically re-fetches whenever the selected user changes
let postsSource = dataSource(selectedUserID.dependency(.eager)) { userID in
    try await api.fetchPosts(forUser: userID)
} onError: { _ in .keep }
.build()
```

Without dependencies, you'd have to observe `selectedUserID` yourself, cancel any in-flight request, and manually call `refresh()`. Silo handles all of that.

## Declaring a Dependency

Call `.dependency(_:)` on any `AsyncStream` to create a ``DataSourceDependency``, then pass it as the first argument to `dataSource()`:

```swift
let filterStream: AsyncStream<SortOrder>

let productsSource = dataSource(filterStream.dependency(.lazy)) { sortOrder in
    try await api.fetchProducts(sortedBy: sortOrder)
} onError: { _ in .keep }
.build()
```

The dependency value becomes a parameter to the fetch closure. Silo ensures the closure always receives the latest emitted value.

> Important: The data source will not perform its first fetch until *all* dependencies have emitted at least one value. If you have two dependencies and one emits before the other, the fetch is held until both have values.

## Refresh Policies

The policy controls *when* a refresh is triggered in response to a dependency change:

### `.eager` — Refresh Immediately

```swift
selectedUserID.dependency(.eager)
```

Refresh happens immediately when the dependency emits, regardless of whether there are active subscribers. Use `.eager` when you want data pre-warmed and ready before any view subscribes — for example, when you anticipate the user navigating to a screen.

### `.lazy` — Refresh Only When Observed

```swift
selectedUserID.dependency(.lazy)
```

If the dependency emits while there are no subscribers, the refresh is deferred. When the first subscriber arrives, the deferred refresh fires immediately, delivering fresh data right away.

Use `.lazy` for data that is only relevant when its corresponding UI is visible. This avoids network calls for screens the user hasn't navigated to yet.

### `.manual` — Manual Control

```swift
selectedUserID.dependency(.manual)
```

Dependency changes do not trigger automatic refreshes. The dependency value is still available in the fetch closure — you just control when `refresh()` is called yourself.

Use `.manual` when you want the fetch closure to *use* the upstream value but need external logic to decide the right time to refresh (for example, after a confirmation dialog).

### Policy Comparison

| Policy | Refresh timing | Best for |
| --- | --- | --- |
| `.eager` | Immediately on change | Pre-warming; always-fresh data |
| `.lazy` | When first subscriber arrives | On-demand screens; avoid unnecessary work |
| `.manual` | Never automatically | Manual control; gated refresh |

## Clearing on Dependency Change

Pass `clear: true` to emit the empty value immediately when the dependency changes — before the new fetch completes:

```swift
selectedUserID.dependency(.eager, clear: true)
```

Without `clear: true`, the previous user's posts remain visible while the new user's posts are being fetched. With `clear: true`, the list clears immediately on switch, showing a loading state until the new data arrives.

| | Without `clear: true` | With `clear: true` |
| --- | --- | --- |
| On change | Previous data shown during fetch | Empty value shown during fetch |
| Best for | Smooth transitions (similar data) | Hard context switches (unrelated data) |

## Multiple Dependencies

Pass multiple dependencies to observe several upstream streams simultaneously. The fetch closure receives all values as parameters:

```swift
let userID:   AsyncStream<Int>
let category: AsyncStream<Category>
let sortOrder: AsyncStream<SortOrder>

let feedSource = dataSource(
    userID.dependency(.lazy),
    category.dependency(.lazy, clear: true),
    sortOrder.dependency(.lazy)
) { userID, category, sortOrder in
    try await api.fetchFeed(
        for: userID,
        in: category,
        sorted: sortOrder
    )
} onError: { _ in .keep }
.build()
```

The data source refreshes whenever *any* dependency emits a new value, using the latest value of all others. The first fetch waits until every dependency has emitted at least once.

> Note: Dependencies are implemented using Swift's variadic generics (`repeat each`), so the type of each dependency value is fully preserved and compiler-checked.

## Pre-fetch Hooks

Reactive dependencies let an upstream stream *push* changes to a data source. Pre-fetch hooks are the complementary pattern: the data source *pulls* from upstream immediately before each fetch.

The primary use case is ensuring a related data source is fresh before your fetch runs:

```swift
let profileSource = dataSource {
    try await api.fetchProfile(token: authSource.value!)
} onError: { _ in .keep }
.beforeFetch(refresh: authSource)  // Refresh auth token first
.build()
```

Every time `profileSource.refresh()` is called, `authSource.refresh()` completes first. Because ``DataSource/refresh(clear:)`` deduplicates in-flight requests, if `authSource` is already refreshing the call simply joins that task — no redundant network request.

### General closure form

For preparation work that isn't a `DataSource` refresh, pass a closure directly:

```swift
let source = dataSource { ... }
    .beforeFetch {
        try await analyticsService.logFetchStarted()
    }
    .build()
```

Multiple `.beforeFetch()` calls run concurrently before the fetch. If any hook throws, the remaining hooks are cancelled:

```swift
dataSource { ... }
    .beforeFetch(refresh: authSource)
    .beforeFetch(refresh: configSource)
    .build()
```

### Failable hooks

If the pre-fetch work is best-effort — a failure shouldn't block the main fetch — use `.failableBeforeFetch()`:

```swift
dataSource { ... }
    .failableBeforeFetch { try await prefetchCache.warm() }
    .build()
```

Any error thrown by a failable hook is silently discarded and the fetch proceeds normally.

### Error handling

When a `.beforeFetch()` hook throws, the error is wrapped in a ``BeforeFetchError`` before being passed to the `onError` handler. This lets you distinguish a pre-fetch failure from a fetch failure:

```swift
dataSource {
    try await api.fetchProfile(token: authSource.value!)
} onError: { error in
    if let beforeFetchError = error as? BeforeFetchError {
        // Auth token refresh failed — keep stale profile
        return .keep
    }
    // Fetch itself failed — clear stale data
    return .clear
}
.beforeFetch(refresh: authSource)
.build()
```

The hook runs after all gate checks (TTL, throttle, debounce, prerequisites) and only once per actual fetch task — concurrent `refresh()` callers that join an in-flight task do not re-run it.

### Pre-fetch hooks vs reactive dependencies

| | Reactive dependency | Pre-fetch hook |
| --- | --- | --- |
| Direction | Upstream pushes → source reacts | Source pulls from upstream before fetching |
| Trigger | Upstream emits a new value | Every `refresh()` that proceeds past the gates |
| Use when | The upstream value is an *input* to the fetch | Another source must be fresh *before* you fetch |

## Combining Dependencies with Time Controls

Dependencies work naturally with debounce and throttle. A typical search pattern:

```swift
let searchQuery: AsyncStream<String>

let resultsSource = dataSource(searchQuery.dependency(.lazy)) { query in
    guard !query.isEmpty else { return [] }
    return try await api.search(query: query)
} onError: { _ in .keep }
.debounce(.milliseconds(300))  // Wait for the user to pause typing
.build()
```

When `searchQuery` emits, Silo triggers a refresh, but the debounce delays execution until 300ms of silence — preventing a fetch on every keystroke.

## The Dependency Lifecycle

```
Dependency emits value
    │
    ├─ .manual ────────────────────── No refresh triggered
    │
    ├─ .eager ─────────────────────── refresh() called immediately
    │                                 (regardless of subscriber count)
    │
    └─ .lazy ─── Has subscribers? ─── Yes ──► refresh() called immediately
                        │
                        No ──────────────────► Flag pending refresh
                                               │
                                         First subscriber arrives
                                               │
                                         refresh() called
```

## When Not to Use Dependencies

Use dependencies when the upstream value is the *input* to a fetch — e.g., the selected user ID determines which user's data to load.

Don't use dependencies as a mechanism to trigger a refresh when *some other thing happens* — for that, just call `refresh()` or `restartAutoRefresh()` directly.

```swift
// ✗ Don't: create a fake stream just to trigger refresh
let triggerStream = AsyncStream<Void> { ... }
let source = dataSource(triggerStream.dependency(.eager)) { _ in
    try await api.fetchData()
} onError: { _ in .keep }
.build()

// ✓ Do: just call refresh() when the triggering condition occurs
let source = dataSource {
    try await api.fetchData()
} onError: { _ in .keep }
.build()

// Later, when the condition occurs:
await source.refresh()
```

## See Also

- <doc:TimeControls>
- ``RefreshPolicy``
- ``DataSourceDependency``
- ``BeforeFetchError``
- <doc:ReactiveDependencies>
