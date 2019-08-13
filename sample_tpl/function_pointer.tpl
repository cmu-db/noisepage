fun dispatch(f: (int32)->int32) -> int32 {
  var x = 10
  return f(x)
}

fun mul2(a: int32) -> int32 {
  return a * 2
}

fun main() -> int32 {
  var j = 10
  return dispatch(mul2)
}
