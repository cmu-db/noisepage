// Test while loops (for loops with condition only)

fun main() -> int64 {
  var ret :int64 = 0
  for (ret < 10) {
    ret = ret + 2
  }
  return ret
}
