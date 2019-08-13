// Test functions on structs
// Should output -20 (= 10 - 30)

struct Point {
  x: int64
  y: int64
}

fun distance(a: *Point, b: *Point, out: *Point) -> nil {
  out.x = a.x - b.x
  out.y = a.y - b.y
}

fun main() -> int64 {
  var a: Point
  var b: Point
  a.x = 10
  a.y = 20
  b.x = 30
  b.y = 40
  var c: Point
  distance(&a, &b, &c)
  return c.x
}
