// INSERT INTO temp_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505
// Select * from temp_table
// Returns the number of tuples inserted (11)
fun main(execCtx: *ExecutionContext) -> int64 {
  // Init inserter
  var col_oids: [1]uint32
  col_oids[0] = 1 // colA

  var col_types: [1]uint32
  col_types[0] = 4 // colA
  var cte_scan_iterator: CteScanIterator
  @cteScanInit(&cte_scan_iterator, execCtx, col_types)

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

    // Insert into temp table
    var insert_temp_table_pr = @cteScanGetInsertTempTablePR(&cte_scan_iterator)
    @prSetInt(insert_temp_table_pr, 0, colA)
    var insert_temp_table_slot = @cteScanTableInsert(&cte_scan_iterator)
  }
  @indexIteratorFree(&index)

  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  oids[0] = 1 // colA
  @tempTableIterInitBind(&tvi, execCtx, "temp_table", oids, &cte_scan_iterator)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
        ret = ret + 1
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
