fun main() -> int64 {
  var year = @extractYear(@dateToSql(2019, 9, 20))

  // Check equality
  if (year != @intToSql(2019)) {
    return 1
  }

  return 0
}