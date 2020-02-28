fun main() -> int64 {
  var sql_int : Integer
  var null_int = @nullToSql(&sql_int)

  if (@isSqlNotNull(null_int)) {
    return -1
  }
  if (!@isSqlNull(null_int)) {
    return -1
  }

  var sql_date : Date
  var null_date = @nullToSql(&sql_date)

  if (@isSqlNotNull(null_date)) {
    return -1
  }
  if (!@isSqlNull(null_date)) {
    return -1
  }

  return 0
}
