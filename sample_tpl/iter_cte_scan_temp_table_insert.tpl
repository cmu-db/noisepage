// Test that Accumulate() returns false on empty
// Returns the number of tuples returned (should be 0)

fun main(exec_ctx: *ExecutionContext) -> int64 {
  // Initialize CTE Scan Iterator

  var col_oids: [1]uint32
  col_oids[0] = 1
  var col_types: [1]uint32
  col_types[0] = 4

  var cte_scan: IterCteScanIterator
  @iterCteScanInit(&cte_scan, exec_ctx, col_types)

  // Iterate from 1 -> 20
  var index_iter: IndexIterator
  @indexIteratorInitBind(&index_iter, exec_ctx, 1, "test_1", "index_1", col_oids)
  var lo_pr = @indexIteratorGetLoPR(&index_iter)
  var hi_pr = @indexIteratorGetHiPR(&index_iter)
  @prSetInt(lo_pr, 0, @intToSql(1))
  @prSetInt(hi_pr, 0, @intToSql(20))

  // Insert values 1, 2, ... 20 into temp table
  for (@indexIteratorScanAscending(&index_iter, 0, 0); @indexIteratorAdvance(&index_iter); ) {
    // Get entry
    var cur_pr = @indexIteratorGetTablePR(&index_iter)
    var cur_val = @prGetInt(cur_pr, 0)
    var slot = @indexIteratorGetSlot(&index_iter)

    // Insert entry
    var insert_pr = @iterCteScanGetInsertTempTablePR(&cte_scan)
    @prSetInt(insert_pr, 0, cur_val)
    var insert_temp_table_slot = @iterCteScanTableInsert(&cte_scan)
  }
  var acc_bool = @iterCteScanAccumulate(&cte_scan)
  // Test accumulate on empty WriteTable
  acc_bool = @iterCteScanAccumulate(&cte_scan)
  @indexIteratorFree(&index_iter)

  var count = 0
  var oids: [1]uint32
  oids[0] = 1

  var seq_iter : TableVectorIterator
  var read_cte_scan = @iterCteScanGetResult(&cte_scan)

  // Count entries in temp table
  @tempTableIterInitBind(&seq_iter, exec_ctx, oids, read_cte_scan)
  for (@tableIterAdvance(&seq_iter)) {
    var pci = @tableIterGetPCI(&seq_iter)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var entry = @pciGetInt(pci, 0)
      count = count + 1
    }
    @pciReset(pci)
  }
  @tableIterClose(&seq_iter)
  @iterCteScanFree(&cte_scan)

  return count
}
