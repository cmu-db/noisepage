// Expected output: 0

fun main() -> int32 {
  var year = @datePart(@dateToSql(-4000, 1, 1), @intToSql(21))

  if (year != -4000) {
    return -1
  }

  year = @datePart(@dateToSql(2010, 1, 1), @intToSql(21))

  if (year != 2010) {
    return -1
  }

  return 0
}
