struct Large {
  a: [10]uint32
}

fun f() -> Large {
  var l: Large
  l.a[0] = 44
  return l
}

fun main() -> uint32 {
  var r = f()
  return r.a[0]
}
