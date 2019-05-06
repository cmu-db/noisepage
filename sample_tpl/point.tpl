struct Point {
  x: int32
  y: int32
}

/*
fun distance(a: Point, b: Point) -> Point {
  var p: Point
  p.x = a.x - b.x
  p.y = a.y - b.y
  return p
}

fun main() -> int32 {
  var a: Point
  var b: Point
  a.x = 10
  a.y = 20
  b.x = 30
  b.y = 40
  var c = distance(a, b)
  return 0
}
*/

fun mul(a: Point) -> int32 {
  return a.y * 10
}

fun main() -> int32 {
  var a: Point
  a.x = 10
  a.y = 20
  return mul(a)
}
