struct output_struct {
  test1_colA: Integer
  test1_colB: Integer
  test2_col1: Integer
  test2_col2: Integer
}

// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA = test_2.col1;
fun main() -> int {
  var out : *output_struct
  for (row1 in test_1) {
    for (row2 in test_2) {
      if (row1.colA == row2.col1) {
        out = @ptrCast(*output_struct, @outputAlloc())
        out.test1_colA = row1.colA
        out.test1_colB = row1.colB
        out.test2_col1 = row2.col1
        out.test2_col2 = row2.col2
        @outputAdvance()
      }
    }
  }
  @outputFinalize()
  return 42
}