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
