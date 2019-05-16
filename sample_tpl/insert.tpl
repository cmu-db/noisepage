struct row_struct {
  col1: Integer
  col2: Integer
}

//don't really know the structure of row structure yet
fun main() -> int {
  var out : *row_struct
  @insert(0,0, &out)
  return 0
}