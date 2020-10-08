// Expected output: 11 (number of output rows)
// SQL: INSERT INTO temp_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505; SELECT * FROM temp_table;

fun main(execCtx: *ExecutionContext) -> int32 {
  var TEMP_OID_MASK: uint64 = 2147483648                       // 2^31

  // Init inserter
  var col_types: [1]uint32
  col_types[0] = 4 // colA
  var temp_col_oids: [1]uint32
  temp_col_oids[0] = TEMP_OID_MASK | 1 // colA
  var cte_scan_iterator: CteScanIterator
  @cteScanInit(&cte_scan_iterator, execCtx, TEMP_OID_MASK, temp_col_oids, col_types)

  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")

  // Iterate through rows with colA between 495 and 505
  // Init index iterator
  var index : IndexIterator
  var index_oid : int32
  var test_oid : int32
  test_oid = @testCatalogLookup(execCtx, "test_1", "")
  index_oid = @testCatalogIndexLookup(execCtx, "index_1")
  @indexIteratorInit(&index, execCtx, 1, test_oid, index_oid, col_oids)
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
  @tableIterInit(&tvi, execCtx, TEMP_OID_MASK, temp_col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
        ret = ret + 1
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  @cteScanFree(&cte_scan_iterator)
  return ret
}
