struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main(execCtx: *ExecutionContext) -> int64 {
  var res = 0
  // output variable
  var out : *output_struct
  // Index iterator
  var index : IndexIterator
  @indexIteratorConstructBind(&index, "test_ns", "test_2", "index_2", execCtx)
  @indexIteratorAddColBind(&index, "test_ns", "test_2", "col1")
  @indexIteratorAddColBind(&index, "test_ns", "test_2", "col2")
  @indexIteratorPerformInit(&index)
  @indexIteratorSetKeySmallInt(&index, 0, @intToSql(50))
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetSmallInt(&index, 1)
    out.colB = @indexIteratorGetIntNull(&index, 0)
    @outputAdvance(execCtx)
    res = res + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return res
}