// Expected output: 0

fun main() -> int {
  var sql_int : Integer
  var null_int = @initSqlNull(&sql_int)

  if (@isSqlNotNull(null_int)) {
    return -1
  }
  if (!@isSqlNull(null_int)) {
    return -1
  }

  var sql_date : Date
  var null_date = @initSqlNull(&sql_date)

  if (@isSqlNotNull(null_date)) {
    return -1
  }
  if (!@isSqlNull(null_date)) {
    return -1
  }

  return 0
}
