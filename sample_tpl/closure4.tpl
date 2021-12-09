// Expected output: 8

fun main() -> int32 {
  // Lambda expressions may contain other lambda expressions
  var timesFour = lambda [] (x: int32) -> int32 {
    var timesTwo = lambda [] (y: int32) -> int32 {
      return y*2
    }
    return timesTwo(x) + timesTwo(x)
  }
  return timesFour(2)
}
