// Expected output: 2
// Check that the return stmt can have an arbitrary expression

fun f() -> int32 {
  return 1
}

fun main() -> int32 {
  return f() + f()
}
