struct index_key {
  col1: Integer
}

struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main() -> int {
  // output variable
  var out : *output_struct
  // key for the index
  var key : index_key
  key.col1 = @intToSql(500)
  // Index iterator
  var index : IndexIterator
  @indexIteratorInit(&index, "index_1")
  @indexIteratorScanKey(&index, @ptrCast(*int8, &key))
  // Attribute to indicate which iterator to use
  for (row in test_1@[index=index]) {
    out = @ptrCast(*output_struct, @outputAlloc())
    out.colA = row.colA
    out.colB = row.colB
    @outputAdvance()
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize()
  return 0
}
