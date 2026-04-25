import Foundation
import Silo
import Testing

@MainActor
@Suite("Default Values Tests", .timeLimit(.minutes(1)))
struct DefaultValuesTests {

  /// Verifies that a `DataSource` whose value type is `Optional` emits `nil` as its initial empty
  /// value before any fetch occurs. Subscribes to `.values` and reads the first emission without
  /// calling `refresh()`, asserting it equals `nil`.
  @Test("Optional type defaults to nil")
  func optionalDefaultsToNil() async throws {
    let source = dataSource {
      return "value" as String?
    } onError: { _ in
      .keep
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    // The outer optional is `nil` only if the stream finished; the inner optional is the actual
    // emitted value. We expect a real emission (.some) whose inner value is nil.
    #expect(initial == .some(nil))
  }

  /// Verifies that a `DataSource` whose value type is `Array` emits an empty array as its initial
  /// empty value before any fetch. Subscribes to `.values` and reads the first emission, asserting
  /// it equals `[]` even though the fetch closure would return a non-empty array.
  @Test("Array type defaults to empty array")
  func arrayDefaultsToEmpty() async throws {
    let source = dataSource {
      return ["item1", "item2"]
    } onError: { _ in
      .keep
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == [])
  }

  /// Verifies that a `DataSource` whose value type is `Dictionary` emits an empty dictionary as its
  /// initial empty value before any fetch. Subscribes to `.values` and reads the first emission,
  /// asserting it equals `[:]` even though the fetch closure would return a populated dictionary.
  @Test("Dictionary type defaults to empty dictionary")
  func dictionaryDefaultsToEmpty() async throws {
    let source = dataSource {
      return ["key": "value"]
    } onError: { _ in
      .keep
    }
    .build()

    var iterator = source.values.makeAsyncIterator()
    let initial = await iterator.next()
    #expect(initial == [:])
  }
}
