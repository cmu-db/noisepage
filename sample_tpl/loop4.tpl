fun main() -> int32 {
  var c = 0
  for (var i = 0; i < 10000; i = i + 1) {
    for (var j = 0; j < i; j = j + 1) {
      c = c + j
    }
  }
  return c
}
