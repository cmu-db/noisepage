fun fib(n: int) -> int {
  if (n < 2) {
    return n
  } else {
    return fib(n-2) + fib(n-1)
  }
}

fun main() -> int {
  return fib(30)
}
