// Check that the return stmt can have an arbitrary expression
// Should return 2.

fun f() -> int32 {
  return 1
}

fun main() -> int32 {
  return f() + f()
}
