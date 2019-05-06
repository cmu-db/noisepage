fun main() -> int {
  var ret :int = 0
  for (row in test_2) {
    if (row.col3 < 500) {
      ret = ret + 1
    }
  }
  return ret
}