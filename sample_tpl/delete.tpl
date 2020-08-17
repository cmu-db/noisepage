// Expected output: 0 (number of tuples in that range after the delete)
// SQL: DELETE FROM test_1 WHERE colA BETWEEN 495 AND 505

fun main(execCtx: *ExecutionContext) -> int32 {
  var count = 0 // output count
  // Init deleter
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  var deleter: StorageInterface

  var test_1_id = @testCatalogLookup(execCtx, "test_1", "")
  @storageInterfaceInit(&deleter, execCtx, test_1_id, col_oids, true)

  // Iterate through rows with colA between 495 and 505
  // Init index iterator
  var index : IndexIterator
  var index_oid = @testCatalogIndexLookup(execCtx, "index_1")
  @indexIteratorInit(&index, execCtx, 1, test_1_id, index_oid, col_oids)
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

    // Delete from table
    if (!@tableDelete(&deleter, &slot)) {
      @indexIteratorFree(&index)
      @storageInterfaceFree(&deleter)
      return 37
    }

    // Delete from index
    var index_pr = @getIndexPR(&deleter, index_oid)
    @prSetInt(index_pr, 0, colA)
    @indexDelete(&deleter, &slot)
  }
  @indexIteratorFree(&index)
  @storageInterfaceFree(&deleter)

  // Check that table does not contain these tuples anymore
  var tvi: TableVectorIterator
  @tableIterInit(&tvi, execCtx, test_1_id, col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
      if (cola >= 495 and cola <= 505) {
        count = count + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)

  return count
}
