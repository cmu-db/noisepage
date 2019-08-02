struct S {
  a: int32
  b: int32
}

fun compare(a: *S, b: *S) -> bool {
  var b = (a.a < b.a)
  return b
}

fun main() -> int32 {
  var s1: S
  var s2: S
  var ret = compare(&s1, &s2)
  return 0
}
