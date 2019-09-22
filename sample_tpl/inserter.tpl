
fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  // Initialization
  var inserter : Inserter
  @inserterInit(&inserter, execCtx, table_oid)
  var pr : *ProjectedRow = @inserterGetTablePR(&inserter)
  @prSetInt(pr, 0, 15)
  @prSetInt(pr, 1, 13)
  @prSetInt(pr, 2, 0)
  @inserterTableInsert(&inserter)

  var index_pr: *ProjectedRow = @inserterGetIndexPR(&inserter, index_oid1)
  @prSetInt(index_pr, 0, @prGetInt(table_pr, 0))
  @inserterIndexInsert(&inserter)
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  @indexIteratorInitBind(&index, execCtx, "test_1", "index_1", col_oids)

  // Next we fill up the index's projected row
  var index_pr = @indexIteratorGetPR(&index)
  @prSetInt(&index_pr, 0, @intToSql(500)) // Set colA

  // Now we iterate through the matches
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @prGetInt(&table_pr, 0)
    out.colB = @prGetInt(&table_pr, 1)
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return count
}