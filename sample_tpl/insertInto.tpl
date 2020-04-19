// Modifies the insert.tpl file to add 100,000 to the value of the tuples just inserted by the query below

// INSERT INTO empty_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505 (should result in 100495 to 100505)
// Returns true if the first value of the updated empty_table has had 100,000 added to it (was properly updated)
// Returns false if not all the values were properly updated
fun main(execCtx: *ExecutionContext) -> bool {
  // Init inserter
  var col_oids: [1]uint32
  col_oids[0] = 1 // colA
  var inserter: StorageInterface
  @storageInterfaceInitBind(&inserter, execCtx, "empty_table", col_oids, true)
  // Iterate through rows with colA between 495 and 505
  // Init index iterator
  var index : IndexIterator
  @indexIteratorInitBind(&index, execCtx, 1, "test_1", "index_1", col_oids)
  // Set iteration bounds
  var lo_index_pr = @indexIteratorGetLoPR(&index)
  var hi_index_pr = @indexIteratorGetHiPR(&index)
  @prSetInt(lo_index_pr, 0, @intToSql(495)) // Set colA in lo
  @prSetInt(hi_index_pr, 0, @intToSql(505)) // Set colA in hi
  for (@indexIteratorScanAscending(&index, 0, 0); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr = @indexIteratorGetTablePR(&index)
    var colA = @prGetInt(table_pr, 0)
    var slot = @indexIteratorGetSlot(&index)

    // Insert into empty_table
    var insert_pr = @getTablePR(&inserter)
    @prSetInt(insert_pr, 0, colA)
    var insert_slot = @tableInsert(&inserter)

    // Update the value at the tuple slot that was just inserted into using insertInto
    @prSetInt(insert_pr, 0, colA + @intToSql(100000))
    @tableInsertInto(&inserter, insert_slot)

    // Insert into index
    var index_pr = @getIndexPRBind(&inserter, "index_empty")
    @prSetInt(index_pr, 0, colA)
    if (!@indexInsert(&inserter)) {
      @indexIteratorFree(&index)
      @storageInterfaceFree(&inserter)
      return false
    }
  }
  @indexIteratorFree(&index)
  @storageInterfaceFree(&inserter)

  // check that the values in the empty table are all between 100495 and 100505
  // Init index iterator
  var verification_index : IndexIterator
  @indexIteratorInitBind(&verification_index, execCtx, 1, "empty_table", "index_1", col_oids)
  @indexIteratorScanAscending(&verification_index, 0, 0)
  var verification_table_pr = @indexIteratorGetTablePR(&verification_index)
  var colA = @prGetInt(table_pr, 0)
  return @sqlToBool(@intToSql(100495) == colA)
}