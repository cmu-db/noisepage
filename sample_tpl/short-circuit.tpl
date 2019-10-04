// Test multiple logical expressions
// Should output 1

fun f(a: int, b: int, c: int) -> int {
  if (a < b and a < c and b < c) {
    return 1
  } else if (a > b and a > c and b > c) {
    return 2
  } else {
    return 3
  }
}

fun main() -> int {
  return f(10,20,30)
}
