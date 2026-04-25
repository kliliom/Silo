# Silo

A lightweight Swift 6 library for managing asynchronous data sources with reactive streams.

## Features

- 🔄 **Reactive Streams** - AsyncStream-based value and state emissions
- 📦 **Smart Caching** - Automatic caching with TTL support
- ⚡️ **Time Controls** - Throttle, debounce, and auto-refresh
- 🔁 **Retry Logic** - Configurable retry strategies with exponential backoff
- 🎯 **Prerequisites** - Conditional fetching based on runtime checks
- 🔗 **Dependencies** - React to changes in other data sources
- 🎭 **Distinct Values** - Filter duplicate emissions
- 🧵 **Swift 6** - Full concurrency support with Sendable conformance

## Installation

Add Silo to your Swift package dependencies:

```swift
dependencies: [
    .package(url: "https://github.com/kliliom/Silo.git", from: "1.0.0")
]
```

## Quick Start

```swift
import Silo

@MainActor
class UserService {
    let userSource: DataSource<User?>

    init() {
        userSource = dataSource {
            try await API.getUser()
        } onError: { error in
            print("Error:", error)
            return .keep     // Keep cached data on error
        }
        .ttl(.seconds(300))  // Cache for 5 minutes
        .distinct()          // Only emit when user changes
        .build()
    }

    var user: AsyncStream<User?> {
        userSource.values
    }

    func refresh() async throws {
        try await userSource.refresh()
    }
}
```

## Core Concepts

### Data Lifecycle

1. **Initial State**: DataSource starts with the empty value (`nil` for Optional, `[]` for Array, etc.)
2. **First Fetch**: Call `refresh()` to fetch data
3. **Caching**: Successful fetch result is cached and emitted to all streams
4. **Updates**: Streams receive updates when data changes

### Basic Example

```swift
let source = dataSource {
    try await API.getData()
} onError: { error in
    return .keep  // or .clear
}
.build()

// Stream values
Task {
    for await value in source.values {
        print("New value:", value)
    }
}

// Manually refresh
try await source.refresh()
```

### State Tracking

Monitor loading and validity state:

```swift
Task {
    for await state in source.state {
        print("Refreshing:", state.isRefreshing)
        print("Empty:", state.isEmpty)
    }
}
```

## Time-Based Features

### TTL (Time To Live)

Cache data for a specific duration:

```swift
dataSource {
    try await API.getConfig()
} onError: { _ in
    .keep
}
.ttl(.seconds(300))  // Cache for 5 minutes
.build()
```

Auto-clear after expiry:

```swift
.ttl(.seconds(900), clear: true)  // Clear data after 15 minutes
```

### Throttle

Limit refresh frequency:

```swift
.throttle(.seconds(2))  // Max one fetch per 2 seconds
```

Execute last call in window:

```swift
.throttle(.seconds(2), last: true)
```

### Debounce

Wait for calls to settle:

```swift
.debounce(.milliseconds(300))  // Wait 300ms after last call
```

### Auto-Refresh

Automatic periodic refreshing:

```swift
.autoRefresh(.seconds(30))  // Refresh every 30 seconds
```

Control at runtime:

```swift
source.stopAutoRefresh()
source.resumeAutoRefresh()
await source.restartAutoRefresh(immediate: true)
```

## Retry Strategies

### Simple Retry

```swift
.retry(count: 3)  // Retry up to 3 times
```

### With Delay

```swift
.retry(count: 3, delay: .seconds(2))  // Wait 2 seconds between retries
```

### Exponential Backoff

```swift
.retry(strategy: .exponentialBackoff(
    maxAttempts: 5,
    initialDelay: .seconds(1),
    multiplier: 2.0,
    maxDelay: .seconds(30)
))
```

### With Error Handler

```swift
.retry(count: 3, delay: .seconds(1)) { error in
    if case APIError.unauthorized = error {
        return .stop   // Stop retrying — defer cache decision to top-level onError
    }
    return .retry  // Continue retrying
}
```

## Distinct Values

Only emit when value actually changes:

```swift
.distinct()  // For Equatable types
```

Custom comparison:

```swift
.distinct { old, new in
    old?.id == new?.id  // Compare by ID only
}
```

## Prerequisites

Check conditions before fetching:

```swift
struct NetworkPrerequisite: DataSourceRefreshPrerequisite {
    func check() async -> Bool {
        // Check network availability
        return await NetworkMonitor.isConnected
    }
}

dataSource {
    try await API.getData()
} onError: { _ in .keep }
.requires(NetworkPrerequisite())
.build()
```

## Dependencies

React to changes in other data sources:

### Single Dependency

```swift
let userSource = dataSource { ... }.build()

let postsSource = dataSource(
    userSource.values.dependency(.eager, clear: true)
) { user in
    guard let user = user else { return [] }
    return try await API.getPosts(userId: user.id)
} onError: { _ in .keep }
.build()
```

### Multiple Dependencies

```swift
let resultsSource = dataSource(
    querySource.values.dependency(.eager, clear: true),
    filtersSource.values.dependency(.eager, clear: true)
) { query, filters in
    try await API.search(query: query, filters: filters)
} onError: { _ in .keep }
.debounce(.milliseconds(300))
.build()
```

### Refresh Policies

- `.eager` - Refresh immediately when dependency changes, even without subscribers
- `.lazy` - Refresh only when there are subscribers. If a dependency changes while there are no subscribers, the refresh is deferred until the first subscriber arrives
- `.manual` - Don't auto-refresh on changes (manual refresh only)

## Error Handling

Two error enums, each used in a different place:

`FetchErrorAction` — returned from the top-level `onError` to control cache contents after all retries are exhausted:

```swift
onError: { error in
    if error is NetworkError {
        return .keep   // Preserve cached data
    } else {
        return .clear  // Reset to empty value
    }
}
```

`RetryErrorAction` — returned from the per-attempt `onError` in `.retry()` to control whether the next retry runs:

```swift
.retry(count: 3) { error in
    return .retry  // Try again, or .stop to defer cache decision to top-level onError
}
```

## Convenience Overloads

Automatic empty values for common types:

```swift
// Optional - defaults to nil
dataSource { ... as String? } onError: { _ in .keep }

// Array - defaults to []
dataSource { ... as [Item] } onError: { _ in .keep }

// Dictionary - defaults to [:]
dataSource { ... as [Key: Value] } onError: { _ in .keep }
```

Custom types require `emptyValue`:

```swift
dataSource {
    try await API.getData()
} onError: { _ in .keep } emptyValue: {
    Data.empty  // Custom empty value
}
```

## SwiftUI Integration

```swift
@MainActor
@Observable
class ViewModel {
    let service = UserService()
    var user: User?
    var isLoading = false

    func observe() {
        Task {
            for await user in service.user {
                self.user = user
            }
        }

        Task {
            for await state in service.userSource.state {
                isLoading = state.isRefreshing
            }
        }
    }

    func refresh() async {
        try? await service.refresh()
    }
}

struct ContentView: View {
    @State var viewModel = ViewModel()

    var body: some View {
        Text(viewModel.user?.name ?? "Loading...")
            .task {
                viewModel.observe()
                try? await viewModel.refresh()
            }
            .refreshable {
                await viewModel.refresh()
            }
    }
}
```

## Advanced Example

Combining multiple features:

```swift
let feedSource = dataSource(
    userSource.values.dependency(.eager, clear: true)
) { user in
    guard let user = user else { return [] }
    return try await API.getFeed(userId: user.id)
} onError: { error in
    print("Feed error:", error)
    return .keep
}
.ttl(.seconds(300))                    // Cache for 5 minutes
.throttle(.seconds(2))               // Limit refresh rate
.debounce(.milliseconds(300))        // Debounce rapid calls
.autoRefresh(.seconds(30))           // Auto-refresh every 30s
.distinct()                          // Filter duplicates
.retry(strategy: .exponentialBackoff(
    maxAttempts: 3,
    initialDelay: .seconds(1),
    multiplier: 2.0
))
.requires(NetworkPrerequisite())     // Only fetch when online
.build()
```

## Requirements

- Swift 6.0+
- iOS 17+, macOS 14+, tvOS 17+, watchOS 10+, visionOS 1+

## Testing

Silo uses Swift Testing:

```bash
swift test
```

## Acknowledgements

Portions of this project — including documentation, and tests — were developed with the assistance of [Claude Code](https://claude.com/claude-code).

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
