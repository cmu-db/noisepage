// Check the return statement can have an arbitrary expression
// Should return 44

struct Large {
  a: [10]int64
}

fun f(l : *Large) -> *Large {
  l.a[0] = 44
  return l
}

fun main() -> int64 {
  var l : Large
  var r = f(&l)
  return r.a[0]
}
