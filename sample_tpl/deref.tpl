fun set(a: *int8, b: *int16, c: *int32, d: *int64) -> void {
  *c = 404
  return
}

fun main() -> int32 {
  var a: int8
  var b: int16
  var c: int32
  var d: int64
  set(&a, &b, &c, &d)
  return c
}
