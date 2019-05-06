fun main() -> int {
  var count = 0
  for (vec in test_1@[batch=2048]) {
    count = count + @filterLt(vec, "test_1.colA", 500)
  }
  return count
}
