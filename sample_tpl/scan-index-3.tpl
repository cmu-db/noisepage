// Performs all types of index scans

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

  // num_attrs being 1 is not a typo.
  @indexIteratorInit(&index, execCtx, 1, test1_oid, index_oid, col_oids)

  // Next we fill up the index's projected row
  var lo_index_pr = @indexIteratorGetLoPR(&index)
  var hi_index_pr = @indexIteratorGetHiPR(&index)
  @prSetInt(lo_index_pr, 0, @intToSql(495)) // Set colA in lo
  @prSetInt(hi_index_pr, 0, @intToSql(505)) // Set colA in hi

  // Iterate through the matches in ascending order: should output 11 tuples (505 - 405 + 1)
  for (@indexIteratorScanAscending(&index, 0, 0); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr1 = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out1 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out1.colA = @prGetInt(table_pr1, 0)
    out1.colB = @prGetInt(table_pr1, 1)
    count = count + 1
  }

  // Iterate through the matches in descending order: should output 11 tuples
  for (@indexIteratorScanDescending(&index); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr2 = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out2.colA = @prGetInt(table_pr2, 0)
    out2.colB = @prGetInt(table_pr2, 1)
    count = count + 1
  }

  // Iterate through matches in ascending order with a limit: should output 5 tuples (limit)
  for (@indexIteratorScanAscending(&index, 0, 5); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr3 = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out3 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out3.colA = @prGetInt(table_pr3, 0)
    out3.colB = @prGetInt(table_pr3, 1)
    count = count + 1
  }

  // Iterate through matches in descending order with a limit: should output 5 tuples
  for (@indexIteratorScanLimitDescending(&index, 5); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr4 = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out4 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    out4.colA = @prGetInt(table_pr4, 0)
    out4.colB = @prGetInt(table_pr4, 1)
    count = count + 1
  }

  // Finalize output
  @indexIteratorFree(&index)
  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return count // total is 32
}
