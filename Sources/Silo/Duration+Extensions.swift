import Foundation

extension Duration {
  /// The duration expressed as a Foundation `TimeInterval` (seconds).
  ///
  /// Used internally to interoperate with `Date`-based APIs such as `addingTimeInterval(_:)`.
  var timeInterval: TimeInterval {
    let (seconds, attoseconds) = self.components
    return TimeInterval(seconds) + TimeInterval(attoseconds) * 1e-18
  }
}
