struct S {
  a: int32
  b: int32
}

fun main() -> int {
  var x: [10]int
  x[1] = 10

  var y: [10]S
  y[1].b = 44
  y[2].a = 1

  return y[y[2].a].b
}
