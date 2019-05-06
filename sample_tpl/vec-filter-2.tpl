fun main() -> int {
  var count = 0
  for (vec in test_2@[batch=2048]) {
    count = count + @filterLt(vec, "test_2.col3", 500)
  }
  return count
}
