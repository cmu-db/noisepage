fun main() -> int64 {
  var present1 = @dateToSql(2019, 9, 20)
  var present2 = @dateToSql(2019, 9, 20)
  var future = @dateToSql(2019, 11, 2)
  var past = @dateToSql(2019, 7, 17)

  // Check equality
  if (present1 != present2 or !(@sqlToBool(present1 == present2))) {
    return 1
  }

  if (present1 >= future or !(@sqlToBool(future > present1))) {
    return 1
  }

  if (present1 <= past or !(@sqlToBool(past < present1))) {
    return 1
  }

  return 0
}