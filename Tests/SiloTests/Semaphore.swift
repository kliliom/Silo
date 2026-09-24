actor Semaphore {
  private var count: Int
  private var waiters: [CheckedContinuation<Void, Never>] = []

  init(value: Int) {
    self.count = value
  }

  func wait() async {
    if count > 0 {
      count -= 1
      return
    }
    await withCheckedContinuation { continuation in
      waiters.append(continuation)
    }
  }

  func signal() {
    if let next = waiters.first {
      waiters.removeFirst()
      next.resume()
    } else {
      count += 1
    }
  }
}

/// Thread-safe counter for tracking fetch and handler invocations in tests.
actor Counter {
  private(set) var value = 0

  func increment() {
    value += 1
  }
}

/// Thread-safe ordered log for recording which values a fetch received.
actor Recorder<Element: Sendable> {
  private(set) var items: [Element] = []

  func append(_ item: Element) {
    items.append(item)
  }
}
