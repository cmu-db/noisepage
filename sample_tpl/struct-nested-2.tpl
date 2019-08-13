// Test nested structs

struct X {
  s: int
  n: int
}

struct Y {
  s: int
  x: X
  x_arr: [10]X
}

fun setTenAtIndex(y: *Y, idx: int32) -> nil {
  y.x_arr[idx].n = 10
}

fun main() -> int {
  var y: Y
  var idx = 4
  setTenAtIndex(&y, idx)
  return y.x_arr[idx].n
}
