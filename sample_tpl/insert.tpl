// Expected output: 11 (the number of tuples inserted)
// SQL: INSERT INTO empty_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505

fun main(execCtx: *ExecutionContext) -> int32 {
  var count = 0 // output count

  // Init inserter
  var inserter: StorageInterface
  var empty_table_oid : int32
  empty_table_oid = @testCatalogLookup(execCtx, "empty_table", "")
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "empty_table", "colA")
  @storageInterfaceInit(&inserter, execCtx, empty_table_oid, col_oids, true)

  // Iterate through rows with colA between 495 and 505
  // Init index iterator
  var index : IndexIterator
  var index1_oid : int32
  index1_oid = @testCatalogIndexLookup(execCtx, "index_1")
  var test1_oid : int32
  test1_oid = @testCatalogLookup(execCtx, "test_1", "")
  @indexIteratorInit(&index, execCtx, 1, test1_oid, index1_oid, col_oids)

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

    // Insert into index
    var index_oid : uint32
    index_oid = @testCatalogIndexLookup(execCtx, "index_empty")
    var index_pr = @getIndexPR(&inserter, index_oid)
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
