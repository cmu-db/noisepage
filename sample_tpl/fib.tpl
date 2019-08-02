fun fib(n: int64) -> int64 {
  if (n < 2) {
    return n
  } else {
    return fib(n-2) + fib(n-1)
  }
}

fun main() -> int64 {
  return fib(20)
}
