// Test for loops

fun main() -> int32 {
  for (var i = 0; i < 10000; i = i + 1) {
    if (i % 31 == 0) {
      return i
    }
  }
  return 0
}
