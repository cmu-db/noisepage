fun main() -> int {
  var x: map[int]int
  x[1] = 10

  var i = 20
  x[i] = 99

  var y: map[int]float
  y[i] = 2.0

  return x[1]
}
