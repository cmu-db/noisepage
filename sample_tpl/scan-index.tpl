// Expected output: 1 (number of output rows)
// SQL: SELECT colA, colB from test_1 WHERE colA = 500;
// Should be done with an index scan, and output "500, 9" to std out.
// The 9 is non-deterministic.

struct output_struct {
  colA: Integer
  colB: Integer
}

fun main(execCtx: *ExecutionContext) -> int {
  var output_buffer = @resultBufferNew(execCtx)
  var count = 0 // output count

  // Initialize the index iterator.
  var test1_oid : int32
  var index : IndexIterator
  var index_oid : int32
  var col_oids: [2]uint32

  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
  test1_oid = @testCatalogLookup(execCtx, "test_1", "")
  index_oid = @testCatalogIndexLookup(execCtx, "index_1")

  @indexIteratorInit(&index, execCtx, 2, test1_oid, index_oid, col_oids)

  // Next we fill up the index's projected row
  var index_pr = @indexIteratorGetPR(&index)
  @prSetInt(index_pr, 0, @intToSql(500)) // Set colA

  // Now we iterate through the matches
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out.colA = @prGetInt(table_pr, 0)
    out.colB = @prGetInt(table_pr, 1)
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return count
}
