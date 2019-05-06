fun main() -> int32 {
  var x = 0
  for (var i = 0; i < 10000000; i = i + 1) {
    if (i % 3 == 0) {
      x = x + i
    }
  }
  return x
}
