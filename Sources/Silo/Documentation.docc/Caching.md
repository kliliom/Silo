# Caching with TTL

Prevent redundant network calls by keeping fetched data valid for a configurable duration.

## Overview

By default every `refresh()` call triggers a fetch. Adding a time-to-live (TTL) window changes this: once data is successfully fetched, `refresh()` returns the cached value immediately — without touching the network — until the window expires.

TTL is the primary tool for reducing server load and improving perceived performance. The right TTL value depends entirely on how quickly your data changes and how stale data you can tolerate.

## Configuring TTL

Chain `.ttl(_:)` on the builder before calling `.build()`:

```swift
let catalogSource = dataSource {
    try await api.fetchProducts()
} onError: { _ in .keep }
.ttl(.seconds(300)) // Data stays fresh for 5 minutes
.build()
```

After a successful fetch, calling `refresh()` within 5 minutes returns the cached product list without a network call. Calling `refresh()` after 5 minutes triggers a new fetch and resets the TTL window.

> Note: Silo uses `Foundation.Duration`, not `Swift.Duration`. Both use identical literal syntax (`.seconds(n)`, `.milliseconds(n)`), but they are different types. Always `import Foundation` alongside `import Silo`.

## Choosing a TTL Value

| Data type | Suggested TTL | Rationale |
| --- | --- | --- |
| User profile, settings | 5–15 minutes | Changes infrequently; stale data is low risk |
| News feed, product catalog | 1–5 minutes | Moderate change rate; some staleness acceptable |
| Prices, availability | 15–60 seconds | Changes often; stale data has real cost |
| Auth tokens, session info | 60–300 seconds | Sensitive; don't hold longer than necessary |
| Real-time data (stock prices, scores) | No TTL + autoRefresh | Cache adds latency you don't want |

## Auto-Clearing on Expiry

By default when the TTL fires, the cache remains in memory and the data source simply becomes eligible for a re-fetch on the next `refresh()` call — no subscriber is notified.

Pass `clear: true` to instead emit the empty value when the TTL fires:

```swift
let sessionSource = dataSource {
    try await api.validateSession()
} onError: { _ in .clear }
.ttl(.seconds(900), clear: true) // Emit empty value after 15 minutes
.build()
```

**Use `clear: true` when** stale data would be misleading or harmful:
- Auth tokens / session state
- Time-sensitive pricing
- Data that becomes invalid rather than just outdated

**Use the default (`clear: false`) when** showing stale data is acceptable:
- News articles, product descriptions
- User profiles
- Historical statistics

## Timer Tolerance

Allow the TTL timer to fire slightly later than requested. The OS can then coalesce wake-ups from multiple timers into a single CPU wake-up, improving battery life:

```swift
.ttl(.seconds(300), tolerance: .seconds(15))
// Fires anywhere between 300 and 315 seconds after the last fetch
```

This maps to the `tolerance` parameter of `Task.sleep(for:tolerance:)`. On macOS/iOS, the system decides the exact firing time within the range. Use a tolerance of 5–10% of the TTL duration as a starting point.

## Bypassing the Cache

Call `refresh(clear: true)` to force a fresh fetch regardless of the TTL window:

```swift
// Pull-to-refresh: always fetch fresh
try await catalogSource.refresh(clear: true)
```

This clears the cached value, emits the empty value, then performs a full fetch. Any active TTL timer is cancelled and restarted after the new fetch completes.

## TTL and Concurrent Fetches

If two callers both call `refresh()` simultaneously when the TTL has expired, only one network request is made — both callers await the same in-flight task. The TTL window is started after that shared task completes.

## TTL, Auto-Refresh, and Caching Together

A common pattern is to configure auto-refresh at a shorter interval than the TTL, so the background timer only actually fetches when the cache has expired:

```swift
let feedSource = dataSource {
    try await api.fetchFeed()
} onError: { _ in .keep }
.ttl(.seconds(120))          // Cache valid for 2 minutes
.autoRefresh(.seconds(60))   // Timer fires every minute, but...
.build()                      // ...first fire is a no-op (still within TTL)
```

In this configuration, the effective refresh rate is once per 2 minutes (governed by the TTL), with the auto-refresh timer acting as a "check-in" mechanism that wakes up the data source regularly.

## Combining TTL with Retry

TTL and retry operate at different layers. TTL determines *whether* to fetch; retry determines what to do *after* a fetch attempt fails:

```swift
let profileSource = dataSource {
    try await api.fetchProfile()
} onError: { _ in .keep }
.ttl(.seconds(300))
.retry(count: 3, delay: .seconds(2)) // Retry failed fetches up to 3 times
.build()
```

If a fetch fails all retry attempts, the cached value is preserved (because `onError` returns `.keep`) and the TTL timer is *not* reset — the next `refresh()` call will trigger another attempt immediately.

## See Also

- <doc:TimeControls>
- <doc:ErrorHandling>
- ``DataSource/refresh(clear:)``
- ``DataSource/clear()``
