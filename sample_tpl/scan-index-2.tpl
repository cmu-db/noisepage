// Perform(using an index):
//
// SELECT col1, col2 from test_2 WHERE col1 = 50
//
// Should return 1 (number of matching tuples)
// Should also output "50, 7" to std out (the output tuple). The "7" is non deterministic.

struct output_struct {
  col1: Integer
  col2: Integer
}

fun main(execCtx: *ExecutionContext) -> int64 {
  var res = 0
  // output variable
  var out : *output_struct
  // Index iterator
  var index : IndexIterator
  var col_oids: [2]uint32
  col_oids[0] = 1 // col1
  col_oids[1] = 2 // col2
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2", col_oids)
  @indexIteratorSetKeySmallInt(&index, 0, @intToSql(50)) // Set index_col1
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    // Note the reordering of the columns
    out.col1 = @indexIteratorGetSmallInt(&index, 1)
    out.col2 = @indexIteratorGetIntNull(&index, 0)
    res = res + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return res
}