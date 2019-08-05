struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main(execCtx: *ExecutionContext) -> int64 {
  // output variable
  var out : *output_struct
  // Index iterator
  var index : IndexIterator
  @indexIteratorConstructBind(&index, "test_1", "index_1", execCtx, "t1")
  @indexIteratorAddColBind(&index, "t1", "colA")
  @indexIteratorAddColBind(&index, "t1", "colB")
  @indexIteratorPerformInitBind(&index, "t1")
  @indexIteratorSetKeyBind(&index, "t1", "index_colA", @intToSql(500))
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetBind(&index, "t1", "colA")
    out.colB = @indexIteratorGetBind(&index, "t1", "colB")
    @outputAdvance(execCtx)
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return 0
}