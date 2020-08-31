fun main() -> int32 {
  var b = @boolToSql(true)
  var i = @convertBoolToInt(b)
  if (i != 1) {
    return 10
  }
  var f = @convertIntToReal(i)
  if (f != 1.0) {
    return 20
  }
  // SQL string->boolean
  var bb = @convertStringToBool(@stringToSql("true"))
  if (bb != true) {
    return 30
  }
  bb =  @convertStringToBool(@stringToSql("F"))
  if (bb != false) {
    return 40
  }
  // SQL string->int
  var ii = @convertStringToInt(@stringToSql("44"))
  if (ii != 44) {
    return 50
  }
  // SQL string->real
  var ff = @convertStringToReal(@stringToSql("123.0"))
  if (ff != 123.0) {
    return 60
  }
  // SQL string->date
  var year = @extractYear(@convertStringToDate(@stringToSql("1994-01-01")))
  if (year != 1994) {
    return 70
  }
  // Success
  return 0
}
