fun f() -> int32 { 
  return 1
}

fun main() -> int32 {
  return f() + f()
}
