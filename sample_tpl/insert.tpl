struct row_struct {
  colA: Integer
}

//don't really know the structure of row structure yet
fun main() -> int {
  var ret: int = 0
  //for (row in empty_table) {
  //  ret = ret + 1
  //}

  var oldret: int = ret

  //do the insert
  var out : row_struct
  out.colA = 0
  @insert(1, 2, &out)

  for (row in empty_table) {
    var l : int = row.colA
    ret = l
  }
  return (ret - oldret)
}