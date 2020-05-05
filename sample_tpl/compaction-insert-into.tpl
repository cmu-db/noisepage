// Allocates new slots and inserts into the block using CompactionInsertInto
// Mimics behaviour of insert.tpl, but inserts into a specific TupleSlot

// INSERT INTO empty_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505
// Returns the number of tuples inserted (11)
fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
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
    var insert_slot = @tableAllocateSlot(&inserter)
    // Insert into the newly allocated slot
    @tableCompactionInsertInto(&inserter, &insert_slot)

    // Insert into index
    var index_pr = @getIndexPRBind(&inserter, "index_empty")
    @prSetInt(index_pr, 0, colA)
    if (!@indexInsert(&inserter)) {
      @indexIteratorFree(&index)
      @storageInterfaceFree(&inserter)
      return 37
    }
    count = count + 1
  }
  @indexIteratorFree(&index)
  @storageInterfaceFree(&inserter)
  return count
}
