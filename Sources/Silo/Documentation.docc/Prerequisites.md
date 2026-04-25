# Prerequisites and Conditional Fetching

Gate fetches on runtime conditions such as network availability or user authentication state.

## Overview

A *prerequisite* is an async check that must pass before a fetch is allowed to proceed. If any prerequisite returns `false`, the fetch is cancelled with a ``PrerequisiteError`` instead of hitting the network.

Prerequisites are the right tool when a fetch shouldn't even be attempted under certain conditions — not when you want to change the *result* of a failed fetch (use `onError` for that).

| | Prerequisites | `onError` |
| --- | --- | --- |
| **Purpose** | Prevent fetches that would certainly fail | React to fetches that did fail |
| **When evaluated** | Before every fetch attempt | After a fetch throws |
| **If check fails** | Throws `PrerequisiteError`; no network call | Decides cache state; re-throws the error |
| **Common uses** | Network offline, not logged in | Transient errors, auth expiry, rate limits |

## Defining a Prerequisite

Conform any `Sendable` type to ``DataSourceRefreshPrerequisite``:

```swift
struct NetworkAvailable: DataSourceRefreshPrerequisite {
    func check() async -> Bool {
        await NetworkMonitor.shared.isConnected
    }
}
```

The `check()` method is `async`, so you can perform any awaitable check — including one that itself involves a network or system call.

## Adding Prerequisites to a Data Source

Chain `.requires(_:)` on the builder before `.build()`. Multiple prerequisites are evaluated in order; the first failure stops the check and throws:

```swift
let profileSource = dataSource {
    try await api.fetchProfile()
} onError: { _ in .keep }
.requires(NetworkAvailable())
.requires(UserAuthenticated())
.build()
```

## Handling Prerequisite Failures

When a prerequisite fails, `refresh()` throws a ``PrerequisiteError``. Catch it to distinguish prerequisite failures from network failures:

```swift
do {
    try await profileSource.refresh()
} catch let error as PrerequisiteError {
    // Condition not met — show an appropriate message
    showOfflineBanner(error.message)
} catch {
    // Actual fetch error
    showErrorAlert(error)
}
```

## Common Prerequisite Implementations

### Network Reachability

```swift
import Network

actor NetworkMonitor {
    static let shared = NetworkMonitor()

    private let monitor = NWPathMonitor()
    private var isConnected = false

    init() {
        monitor.pathUpdateHandler = { [weak self] path in
            Task { await self?.update(path.status == .satisfied) }
        }
        monitor.start(queue: .global(qos: .background))
    }

    private func update(_ connected: Bool) { isConnected = connected }
    func checkConnected() async -> Bool { isConnected }
}

struct NetworkAvailable: DataSourceRefreshPrerequisite {
    func check() async -> Bool {
        await NetworkMonitor.shared.checkConnected()
    }
}
```

### User Authentication

```swift
struct UserAuthenticated: DataSourceRefreshPrerequisite {
    func check() async -> Bool {
        await AuthStore.shared.isLoggedIn
    }
}
```

### Feature Flag

```swift
struct FeatureEnabled: DataSourceRefreshPrerequisite {
    let flagName: String

    func check() async -> Bool {
        await FeatureFlags.shared.isEnabled(flagName)
    }
}

// Usage:
dataSource { ... }
    .requires(FeatureEnabled(flagName: "newFeedAlgorithm"))
    .build()
```

### Combined Example

```swift
let secureDataSource = dataSource {
    try await api.fetchSecureData()
} onError: { error in
    error is AuthError ? .clear : .keep
}
.requires(NetworkAvailable())
.requires(UserAuthenticated())
.requires(FeatureEnabled(flagName: "secureFeature"))
.retry(count: 2, delay: .seconds(1)) { error in
    error is URLError ? .retry : .keep
}
.ttl(.seconds(300))
.build()
```

In this configuration:
1. Network is checked first — no request attempted if offline
2. Auth is verified second — no request if logged out
3. Feature flag is checked third — no request if feature is off
4. If all pass and the fetch fails, retry up to 2 times
5. Cache results for 5 minutes

## Prerequisite Evaluation Order

Prerequisites are evaluated sequentially in the order they were added. Evaluation stops at the first failure — subsequent prerequisites are not checked.

```swift
.requires(NetworkAvailable())   // Checked first
.requires(UserAuthenticated())  // Only checked if network is available
.requires(FeatureEnabled(...))  // Only checked if both above pass
```

## Prerequisites and Retry

Prerequisite checks happen *before* the retry loop. If a prerequisite fails, the fetch is not attempted — and the retry strategy never kicks in. This is intentional: retrying a fetch when the prerequisite is already known to be unmet would be wasteful.

If you need to retry until a prerequisite passes (for example, waiting for a network connection to be restored), handle that at a higher level — poll or observe the prerequisite's underlying state and call `refresh()` when it changes.

## See Also

- <doc:ErrorHandling>
- ``DataSourceRefreshPrerequisite``
- ``PrerequisiteError``
- ``DataSourceBuilder/requires(_:)``
