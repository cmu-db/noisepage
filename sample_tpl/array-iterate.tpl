// Try to iterate over an array
// Should output 110

// Fill the input array with 'n' multiples of two
fun fillWithMultiplesOfTwo(arr: [*]int64, n: int64) -> nil {
  for (var i = 0; i < n; i = i + 1) {
    arr[i] = (i + 1) * 2
  }
}

// Sum all elements in the array
fun sumElems(arr: [*]int64, n: int64) -> int64 {
  var ret = 0
  for (var i = 0; i < n; i = i + 1) {
    ret = ret + arr[i]
  }
  return ret
}

fun main() -> int64 {
  var arr: [10]int64
  // Fill array with multiples of two
  fillWithMultiplesOfTwo(&arr, 10)
  // arr = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20), thus sum = 110
  return sumElems(&arr, 10)
}
