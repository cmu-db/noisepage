fun main() -> int64 {
  var str1 = @stringToSql("StrAing")
  var copy = @stringToSql("StrAing")
  var str2 = @stringToSql("StrBing")

  if (str1 != copy) {
    return 1
  }

  if (str1 >= str2 or !(@sqlToBool(str2 > str1))) {
    return 2
  }

  return 0
}