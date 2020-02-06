fun main() -> int64 {
  var present1 = @timestampToSqlHMSu(2019, 1, 2, 11, 22, 33, 120)
  var present2 = @timestampToSqlHMSu(2019, 1, 2, 11, 22, 33, 120)
  var future = @timestampToSqlHMSu(2019, 1, 2, 11, 22, 33, 121)
  var past = @timestampToSqlHMSu(2019, 1, 2, 11, 22, 33, 119)

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
