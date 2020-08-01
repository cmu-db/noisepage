// Expected output: 0

fun main() -> int32 {
  var date = @dateToSql(-4000, 1, 1)
  var year = @extractYear(date)

  if (year != -4000) {
    return -1
  }

  date = @dateToSql(2010, 1, 1)
  year = @extractYear(date)

  if (year != 2010) {
    return -1
  }

  return 0
}
