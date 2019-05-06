fun main() -> int {
  var ret :int = 0
  for (row in test_1) {
    if (row.colB < row.colA) {
      ret = ret + 1
    }
  }
  return ret
}