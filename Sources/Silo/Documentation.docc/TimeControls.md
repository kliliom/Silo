# Time Controls

Limit fetch frequency with throttling, smooth out bursts with debouncing, and keep data current with auto-refresh.

## Overview

Silo provides three complementary time-based policies. Each addresses a different problem:

| Policy | Problem it solves | Applied at |
| --- | --- | --- |
| **Throttle** | Prevents too many fetches in a short window | Per fetch call |
| **Debounce** | Waits for rapid calls to settle before fetching | Per fetch call |
| **Auto-refresh** | Keeps data current without manual triggers | Background |

All three can be combined with TTL caching — if cached data is still fresh, neither throttle nor auto-refresh will trigger an actual network call.

## Throttle

Throttling enforces a minimum time between fetches. This is the right tool when a fetch can be triggered by rapid user interaction (button taps, scroll-driven prefetching) and you want a hard rate limit regardless of how many calls arrive.

```swift
let refreshSource = dataSource {
    try await api.fetchUpdates()
} onError: { _ in .keep }
.throttle(.seconds(2)) // At most one fetch per 2 seconds
.build()
```

### Drop vs. Queue Mode

Two behaviours are available, selected with the `last` parameter:

**Drop mode** (`last: false`, the default): the first call in a window fetches immediately. Subsequent calls during the window return the cached value without fetching. The pending calls are silently discarded when the window ends.

```swift
.throttle(.seconds(2))         // First call goes through; others return cache
```

**Queue mode** (`last: true`): the first call fetches. Additional calls during the window are deferred. When the window expires, the *most recent* queued call is executed.

```swift
.throttle(.seconds(2), last: true) // Last caller always gets fresh data eventually
```

**When to choose:**

| | Drop (`last: false`) | Queue (`last: true`) |
| --- | --- | --- |
| Manual refresh button | ✓ First tap always works | |
| Search-as-you-type | | ✓ Last query always resolves |
| Sensor polling | ✓ Discard intermediate readings | |
| Form auto-save | | ✓ Latest input always saves |

### Throttle Tolerance

Allow the wait timer in queue mode to fire slightly late:

```swift
.throttle(.seconds(2), last: true, tolerance: .milliseconds(200))
```

## Debounce

Debouncing delays execution until calls have stopped arriving for a specified quiet period. Each `refresh()` call resets the timer. Only after the full quiet period elapses does the actual fetch happen.

```swift
let searchSource = dataSource { query in
    try await api.search(query: query)
} onError: { _ in .keep }
emptyValue: { [] }
.debounce(.milliseconds(350)) // Wait 350ms of silence before fetching
.build()
```

If `refresh()` is called three times in 200ms, the first two calls are cancelled and only the third one — after 350ms of silence — triggers a fetch.

> Tip: 250–400ms is a sweet spot for search debouncing. Values under 150ms feel laggy; values over 500ms feel slow.

### Debounce vs. Throttle Decision Guide

| Situation | Use |
| --- | --- |
| Search-as-you-type | Debounce — wait for the user to pause |
| Refresh button taps | Throttle — first tap always works |
| Rapid scroll triggers | Throttle — drop intermediate positions |
| Text field auto-complete | Debounce — only query after typing stops |
| Pull-to-refresh gesture | Neither — call `refresh(clear: true)` directly |

### Debounce and Cancellation

Callers that are cancelled by a subsequent `refresh()` call during the debounce window receive a `CancellationError`. Handle this if needed:

```swift
do {
    try await searchSource.refresh()
} catch is CancellationError {
    // Expected — this call was superseded by a newer one
} catch {
    showError(error)
}
```

## Auto-Refresh

Auto-refresh periodically calls `refresh()` at a fixed interval while the data source has at least one active subscriber to `values` or `valueWithState`. When the first subscriber appears, an immediate refresh is triggered before the first interval elapses — so data is never stale on first observation. The timer stops automatically when the last subscriber terminates.

```swift
let stockSource = dataSource {
    try await api.fetchPrice(ticker: "AAPL")
} onError: { _ in .keep }
.autoRefresh(.seconds(10)) // Poll every 10 seconds while being observed
.build()
```

```swift
// Timer starts here:
for await price in stockSource.values {
    priceLabel.text = price.formatted(.currency(code: "USD"))
} // Timer stops here (stream ended)
```

This subscriber-driven design means no background work happens when the UI is off-screen.

> Important: Auto-refresh calls `refresh()` internally, so all configured policies still apply — TTL, throttle, and debounce all take effect. If data is fresh within its TTL, an auto-refresh call is a no-op that returns the cached value.

### Managing Auto-Refresh Manually

Pause and resume the timer explicitly, for example when the app enters the background:

```swift
// AppDelegate / SceneDelegate
func sceneDidEnterBackground(_ scene: UIScene) {
    stockSource.stopAutoRefresh()
}

func sceneWillEnterForeground(_ scene: UIScene) {
    stockSource.resumeAutoRefresh()
}
```

``DataSource/resumeAutoRefresh()`` resumes the timer and triggers an immediate refresh, ensuring data is not stale for a full interval after the app returns to the foreground.

To reset the interval from zero (and optionally fire an immediate refresh):

```swift
// Reset the 10-second cycle without an immediate fetch
await stockSource.restartAutoRefresh(immediate: false)

// Reset and fetch right now (useful when returning from background)
await stockSource.restartAutoRefresh(immediate: true)
```

### Auto-Refresh and Battery

Set a tolerance to allow the OS to coalesce wake-ups from multiple timers, reducing CPU wake frequency:

```swift
.autoRefresh(.seconds(60), tolerance: .seconds(5))
// Timer fires between 60 and 65 seconds after each refresh
```

For data that refreshes every minute or more, a 5–10% tolerance has negligible UX impact but measurable battery benefit on mobile devices.

## Combining All Three

All three policies can coexist. Evaluation order within a single `refresh()` call:

```
1. TTL check  →  2. Debounce wait  →  3. Throttle check  →  4. Fetch
```

If TTL passes (data is fresh), the cached value is returned immediately and the rest of the pipeline is skipped — no debounce wait, no throttle delay, no fetch.

A complete example for a live sports scoreboard:

```swift
let scoreSource = dataSource {
    try await api.fetchLiveScores()
} onError: { _ in .keep }
.debounce(.milliseconds(100))   // Absorb rapid-fire triggers from events
.ttl(.seconds(5))               // Don't re-fetch within 5 seconds
.throttle(.seconds(2))          // Hard rate limit: max 1 fetch per 2 seconds
.autoRefresh(.seconds(15))      // Background refresh every 15 seconds
.build()
```

## See Also

- <doc:Caching>
- ``DataSource/stopAutoRefresh()``
- ``DataSource/resumeAutoRefresh()``
- ``DataSource/restartAutoRefresh(immediate:)``
