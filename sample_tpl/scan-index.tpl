struct index_key {
  col1: Integer
}

struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main(execCtx: *ExecutionContext) -> int {
  // output variable
  var out : *output_struct
  // key for the index
  var key : index_key
  key.col1 = @intToSql(500)
  // Index iterator
  var index : IndexIterator
  @indexIteratorInit(&index, "test_1", "index_1", execCtx)
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index, @ptrCast(*int8, &key)); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetInt(&index, 0)
    out.colB = @indexIteratorGetInt(&index, 1)
    @outputAdvance(execCtx)
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return 0
}
