// UPDATE test_1 SET colA = 100000 + colA WHERE colA BETWEEN 495 AND 505
// Returns the tuples with values BETWEEN 100495 AND 100505 (11)

fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // Init updater
  var col_oids: [4]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  col_oids[2] = 3 // colC
  col_oids[3] = 4 // colD
  var updater: StorageInterface
  @storageInterfaceInitBind(&updater, execCtx, "test_1", col_oids, true)
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

    // Delete + Insert on Table
    if (!@tableDelete(&updater, &slot)) {
      @indexIteratorFree(&index)
      @storageInterfaceFree(&updater)
      return 37
    }
    var insert_pr = @getTablePR(&updater)
    @prSetInt(insert_pr, 0, colA + @intToSql(100000))
    var insert_slot = @tableInsert(&updater)

    // Delete + Index on Index
    var index_pr = @getIndexPRBind(&updater, "index_1")
    @prSetInt(index_pr, 0, colA)
    @indexDelete(&updater, &slot)
    @prSetInt(index_pr, 0, colA + @intToSql(100000))
    if (!@indexInsert(&updater)) {
      @indexIteratorFree(&index)
      @storageInterfaceFree(&updater)
      return 38
    }
  }
  @indexIteratorFree(&index)
  @storageInterfaceFree(&updater)

  // Count the number of updated tables
  var tvi: TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
      if (cola >= 100495 and cola <= 100505) {
        count = count + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)

  return count
}
