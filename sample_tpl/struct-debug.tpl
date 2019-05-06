struct S {
  a : int
  b : int
}

fun main() -> int {
  var s : S
  s.a = 0
  for (s.a < 100000) {
    s.a = s.a + 1
  }
  return s.a
}
