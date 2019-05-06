struct output_struct {
  col1: Integer
  col2: Integer
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main() -> int {
  var out : *output_struct
  for (row in test_1) {
    if (row.colA < 500) {
      out = @ptrCast(*output_struct, @outputAlloc())
      out.col1 = row.colA
      out.col2 = row.colB
      @outputAdvance()
    }
  }
  @outputFinalize()
  return 0
}