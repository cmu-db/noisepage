fun main() -> int {
  var ret = 0
  for (row in test_1) {
    if ((row.colA >= 50 and row.colB < 10000000) or (row.colC < 500000)) {
      ret = ret + 1
    }
  }
  return ret
}
