// Note that rows are sorted by size here
struct index_key {
  colB: Integer
  colA: Integer
}

struct output_struct {
  test1_col1: Integer
  test2_col1: Integer
}

// SELECT test_1.colA, test_2.col1 FROM test_1, test_2 WHERE test_1.colA=test_2.col1 AND test_1.colB=test_2.col2
// The two columns outputted should be the same
fun main() -> int {
  // output variable
  var out : *output_struct
  // key for the index
  var key : index_key
  // Index iterator
  var index : IndexIterator
  @indexIteratorInit(&index, "index_2")
  // Attribute to indicate which iterator to use
  for (row1 in test_1) {
    // Copy the join columns into the index key.
    key.colA = row1.colA
    key.colB = row1.colB
    @indexIteratorScanKey(&index, @ptrCast(*int8, &key))
    for (row2 in test_2@[index=index]) {
      out = @ptrCast(*output_struct, @outputAlloc())
      out.test1_col1 = row1.colA
      out.test2_col1 = row2.col1
      @outputAdvance()
    }
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize()
  return 0
}