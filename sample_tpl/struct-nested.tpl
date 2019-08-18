// Test nested structs

struct X {
  s: int
  n: int
}

struct Y {
  s: int
  x: X
}

fun main() -> int {
  var y: Y
  y.x.n = 10
  return y.x.n
}
