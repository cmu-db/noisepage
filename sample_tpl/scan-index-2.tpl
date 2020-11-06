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

fun main(execCtx: *ExecutionContext) -> int {
  var output_buffer = @resultBufferNew(execCtx)
  var res = 0

  // Initialize the index iterator.
  var test2_oid : int32
  var index : IndexIterator
  var index_oid : int32
  var col_oids: [2]uint32

  col_oids[0] = @testCatalogLookup(execCtx, "test_2", "col1")
  col_oids[1] = @testCatalogLookup(execCtx, "test_2", "col2")
  test2_oid = @testCatalogLookup(execCtx, "test_2", "")
  index_oid = @testCatalogIndexLookup(execCtx, "index_2")

  @indexIteratorInit(&index, execCtx, 2, test2_oid, index_oid, col_oids)

  // Fill up index PR
  var index_pr = @indexIteratorGetPR(&index)
  @prSetSmallInt(index_pr, 0, @intToSql(50)) // Set index_col1

  // Iterate
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    // Materialize
    var table_pr = @indexIteratorGetTablePR(&index)

    // Output (note the reordering of the columns)
    var out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out.col1 = @prGetSmallInt(table_pr, 1)
    out.col2 = @prGetIntNull(table_pr, 0)
    res = res + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return res
}
