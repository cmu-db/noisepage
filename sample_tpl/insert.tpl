struct row_struct {
  colA: Integer
  colB: Integer
  colC: Integer
  colD: Integer
}

//don't really know the structure of row structure yet
fun main() -> int {
  var out : *row_struct
  out.colA = 11
  out.colB = 22
  out.colC = 33
  out.colD = 44
  //@insert(1, 7, &out)
  return 0
}