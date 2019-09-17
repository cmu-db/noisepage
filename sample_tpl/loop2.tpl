// Test for loops

fun main() -> int32 {
  var x = 0
  for (var i = 0; i < 100000; i = i + 1) {
    if (i % 3 == 0) {
      x = x + i
    }
  }
  return x
}
