fun main() -> int {
  var ret :int = 0
  for (row in test_1) {
    if (row.colA < 500) {
      ret = ret + 1
    }
  }
  return ret
}
