fun main() -> int64 {
  var present1 = @timestampToSqlYMDHMSMU(2019, 1, 2, 11, 22, 33, 120)
  var present2 = @timestampToSqlYMDHMSMU(2019, 1, 2, 11, 22, 33, 120)
  var future = @timestampToSqlYMDHMSMU(2019, 1, 2, 11, 22, 33, 121)
  var past = @timestampToSqlYMDHMSMU(2019, 1, 2, 11, 22, 33, 119)

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
