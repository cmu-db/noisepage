// Test struct pointer fields

struct S {
  a: int
  b: int
}

fun f(s: *S) -> int {
  s.b = s.a * 44
  return s.b
}

fun main() -> int {
  var s: S
  s.a = 10
  f(&s)
  return s.b
}
