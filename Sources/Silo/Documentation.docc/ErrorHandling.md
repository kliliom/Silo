# Error Handling and Retry

Decide what happens to cached data on failure, and configure automatic retry with backoff strategies.

## Overview

Error handling in Silo is split across two distinct types, each with its own responsibility:

| Type | Used in | Controls |
| --- | --- | --- |
| ``FetchErrorAction`` | Top-level `onError` closure | Cache contents after all retries are exhausted |
| ``RetryErrorAction`` | Per-attempt `onError` in `.retry()` | Whether to attempt the next retry |

This separation keeps each handler to one job: the retry `onError` decides how hard to fight before giving up; the top-level `onError` decides what the user sees after failure. Cache disposition lives in exactly one place.

## The Top-Level `onError` Handler

The `onError` closure on `dataSource()` receives the error thrown after all retries are exhausted and returns an ``FetchErrorAction``:

```swift
let profileSource = dataSource {
    try await api.fetchProfile()
} onError: { error in
    switch error {
    case is NetworkOfflineError:
        return .keep   // Show stale profile while offline
    case is AuthError:
        return .clear  // Wipe sensitive data on auth failure
    default:
        return .keep
    }
}
.build()
```

``FetchErrorAction`` has two cases:

| Action | Cache result | Use when |
| --- | --- | --- |
| `.keep` | Unchanged | Stale data is better than nothing |
| `.clear` | Replaced with empty value | Stale data is wrong or sensitive |

The error is always re-thrown after the handler runs, so callers can still `catch` it:

```swift
do {
    try await profileSource.refresh()
} catch let error as AuthError {
    showLoginScreen()
} catch {
    showErrorBanner(error.localizedDescription)
}
```

## Simple Retry

Retry up to N times with a constant delay between attempts:

```swift
dataSource { ... }
    .retry(count: 3, delay: .seconds(1))
    .build()
// Attempt 1 → fail → wait 1s → Attempt 2 → fail → wait 1s → Attempt 3 → fail → onError
```

## Retry with Per-Attempt Error Gating

Supply a closure returning ``RetryErrorAction`` to decide per-error whether to keep retrying:

```swift
dataSource { ... }
    .retry(count: 3, delay: .seconds(1)) { error in
        switch error {
        case is URLError:       return .retry  // Network error — try again
        case is DecodingError:  return .stop   // Parsing error — give up immediately
        default:                return .stop
        }
    }
    .build()
```

``RetryErrorAction`` has two cases:

| Action | Effect | Use when |
| --- | --- | --- |
| `.retry` | Wait the delay, then attempt again | Error is transient and worth retrying |
| `.stop` | Stop retrying; defer cache decision to top-level `onError` | Error is permanent; further retries would waste effort |

When `.stop` is returned, retrying halts immediately and the error flows to the top-level `onError` handler, which decides whether to keep or clear the cache.

## Exponential Backoff

Each retry waits exponentially longer than the previous. This is the right choice for rate-limited APIs or servers under load:

```swift
dataSource { ... }
    .retry(strategy: .exponentialBackoff(
        maxAttempts: 5,
        initialDelay: .seconds(1),
        multiplier: 2.0
    ))
    .build()
// Delays: 1s → 2s → 4s → 8s → onError
```

Cap the maximum wait to avoid very long delays:

```swift
.retry(strategy: .exponentialBackoff(
    maxAttempts: 6,
    initialDelay: .seconds(1),
    multiplier: 2.0,
    maxDelay: .seconds(30)
))
// Delays: 1s → 2s → 4s → 8s → 16s → 30s (capped) → onError
```

## Linear Backoff

Each retry adds a fixed increment to the previous delay:

```swift
.retry(strategy: .linearBackoff(
    maxAttempts: 4,
    initialDelay: .seconds(2),
    increment: .seconds(3)
))
// Delays: 2s → 5s → 8s → onError
```

## Custom Retry Strategy

Build any delay pattern with a `delayCalculator` closure:

```swift
// Jittered exponential backoff
let jitteredBackoff = RetryStrategy.custom(maxAttempts: 5) { attempt in
    let base = pow(2.0, Double(attempt - 1))
    let jitter = Double.random(in: 0.8...1.2)
    return .seconds(base * jitter)
}

dataSource { ... }
    .retry(strategy: jitteredBackoff)
    .build()
```

> Note: The `delayCalculator` closure is called with the current attempt number (1-indexed). Attempt 1 is the first retry, after the initial failure.

## Combining Strategy and Error Gating

```swift
dataSource { ... }
    .retry(
        strategy: .exponentialBackoff(maxAttempts: 4, initialDelay: .seconds(1)),
        onError: { error in
            if error is RateLimitError   { return .retry }
            if error is ServerError      { return .retry }
            return .stop  // Client errors and decoding failures: stop retrying
        }
    )
    .build()
```

## Retry Tolerance

All retry variants accept a `tolerance` parameter to allow `Task.sleep` to fire slightly late, improving battery efficiency:

```swift
.retry(count: 3, delay: .seconds(5), tolerance: .milliseconds(500))
```

## Interaction Between the Two `onError` Handlers

```
fetch() throws
    │
    └── retry onError? ──.retry──► wait delay ──► fetch() [next attempt]
                │                                       │
                │         ◄──── max attempts reached ───┘
                │
                └──.stop ──► stop retrying
                                    │
                                    ▼
                           top-level onError
                           ├── .keep  → preserve cache
                           └── .clear → emit empty value
                                    │
                                    ▼
                           error re-thrown to caller
```

## See Also

- <doc:Caching>
- ``FetchErrorAction``
- ``RetryErrorAction``
- ``RetryStrategy``
- ``DataSource/refresh(clear:)``
