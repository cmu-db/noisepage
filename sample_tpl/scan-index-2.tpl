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
  // Initialize index iterator
  var index : IndexIterator
  var col_oids: [2]uint32
  col_oids[0] = 1 // col1
  col_oids[1] = 2 // col2
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2", col_oids)

  // Fill up index PR
  var index_pr = @indexIteratorGetPR(&index)
  @prSetSmallInt(&index_pr, 0, @intToSql(50)) // Set index_col1

  // Iterate
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    // Materialize
    var table_pr = @indexIteratorGetTablePR(&index)

    // Output (note the reordering of the columns)
    var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.col1 = @prGetSmallInt(&table_pr, 1)
    out.col2 = @prGetIntNull(&table_pr, 0)
    res = res + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return res
}