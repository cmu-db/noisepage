// Expected output: 0

// This function returns 2020-08-03 11:22:33:123.
fun ts_some_day_in_the_past() -> Timestamp {
  return @timestampToSqlYMDHMSMU(2020, 8, 3, 11, 22, 33, 123, 0)
}

// The int_bustubN() family of functions all return 15445.
fun int_bustub() -> Integer {
  return @intToSql(15445)
}
fun int_bustub2() -> Integer {
  var x = int_bustub()
  return x
}
fun int_bustub3() -> Integer {
  var x = int_bustub2()
  return x
}
fun int_bustub4() -> Integer {
  var x = int_bustub3()
  return x
}
fun int_bustub5() -> Integer {
  var x = int_bustub4()
  return x
}

// The int_Nbustub() family of functions all return N * int_bustub().
fun int_3bustub() -> Integer {
  var x = int_bustub5()
  var y = int_bustub2()
  return x + 2 * y
}

fun main() -> int {
  var past = ts_some_day_in_the_past()
  var present = @timestampToSqlYMDHMSMU(2020, 8, 3, 11, 22, 33, 123, 456)

  if (!(past < present) or (past > present)) {
    return 1
  }

  if (past != @timestampToSqlYMDHMSMU(2020, 8, 3, 11, 22, 33, 123, 0)) {
    return 1
  }

  var x = int_3bustub()
  if (x != @intToSql(46335)) {
    return 1
  }

  return 0
}
