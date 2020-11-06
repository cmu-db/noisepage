// Expected output: 0

fun main() -> int {
  var year = @datePart(@dateToSql(2019, 9, 20), @intToSql(21))

  // Check equality
  if (year != @intToSql(2019)) {
    return 1
  }

  return 0
}