// Expected output: 54

struct Test {
  a : uint8
  b : uint16
  c : uint32
  d : uint64
  e : float32
  f : float64
}

fun main() -> int {
    // offset of a = 0
    // offset of b = 2
    // offset of c = 4
    // offset of d = 8
    // offset of e = 16
    // offset of f = 24
    return @offsetOf(Test, a) +
           @offsetOf(Test, b) +
           @offsetOf(Test, c) +
           @offsetOf(Test, d) +
           @offsetOf(Test, e) +
           @offsetOf(Test, f)
}