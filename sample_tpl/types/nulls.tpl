// Expected output: 0

fun main() -> int {
  var sql_int : Integer
  var null_int = @initSqlNull(&sql_int)

  if (!@isValNull(null_int)) {
    return -1
  }
  var sql_date : Date
  var null_date = @initSqlNull(&sql_date)

  if (!@isValNull(null_date)) {
    return -1
  }

  return 0
}
