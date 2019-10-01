// Tests comparisons
// Should output 1

struct S {
  a: int64
  b: int64
}

fun compare(a: *S, b: *S) -> bool {
  var ret : bool = (a.a > b.a)
  return ret
}

fun main() -> int64 {
  var s1: S
  s1.a = 10
  s1.b = 10
  var s2: S
  s2.a = 20
  s2.b = 20
  if (!compare(&s1, &s2)) {
    return 1
  }
  return -1
}
