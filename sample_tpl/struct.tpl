struct S {
  a: int
  b: int
  c: int
}
struct SDup {
  d: int
  e: int
  f: int
}

fun main() -> int {
  var p: S
  var q: S

  for (var i = 0; i < 100000; i = i + 1) {
    q.a = 1
    q.b = 2
    p.a = 3
    p.b = 4
    q.c = p.a + p.b + q.a + q.b
    p.c = q.c + p.b
  }

  return q.c
}
