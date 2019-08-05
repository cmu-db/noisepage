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
  @indexIteratorConstructBind(&index, "test_2", "index_2", execCtx, "t2")
  @indexIteratorAddColBind(&index, "t2", "col1")
  @indexIteratorAddColBind(&index, "t2", "col2")
  @indexIteratorPerformInitBind(&index, "t2")
  @indexIteratorSetKeyBind(&index, "t2", "index_col1", @intToSql(50))
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetBind(&index, "t2", "col1")
    out.colB = @indexIteratorGetBind(&index, "t2", "col2")
    @outputAdvance(execCtx)
    res = res + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return res
}