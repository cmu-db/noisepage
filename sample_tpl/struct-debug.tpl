// Test struct fields

struct S {
  a : int64
  b : int64
}

fun main() -> int64 {
  var s : S
  s.a = 0
  for (s.a < 100000) {
    s.a = s.a + 1
  }
  return s.a
}
