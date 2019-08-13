// Test struct fields

struct S {
  z: int
  y: int
  a: int
}

fun main() -> int {
  var q : S
  q.a = 10
  q.z = 20
  return q.a + q.z
}
