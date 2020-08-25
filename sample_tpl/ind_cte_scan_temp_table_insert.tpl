// Test that Accumulate() returns false on empty
// Returns the number of tuples returned (should be 0)

fun main(exec_ctx: *ExecutionContext) -> int32 {
  // Initialize CTE Scan Iterator

  var col_oids: [1]uint32
  col_oids[0] = 1
  var col_types: [1]uint32
  col_types[0] = 4

  var temp_col_oids: [1]uint32
  temp_col_oids[0] = -2147483647 // colA

  var cte_scan: IndCteScanIterator
  @indCteScanInit(&cte_scan, exec_ctx, -2147483648, temp_col_oids, col_types, false)

  // Iterate from 1 -> 20
  var index_iter : IndexIterator
  var index_oid : int32
  var test_oid : int32
  test_oid = @testCatalogLookup(exec_ctx, "test_1", "")
  index_oid = @testCatalogIndexLookup(exec_ctx, "index_1")
  @indexIteratorInit(&index_iter, exec_ctx, 1, test_oid, index_oid, col_oids)
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
    var insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
    @prSetInt(insert_pr, 0, cur_val)
    var insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)
  }
  var acc_bool = @indCteScanAccumulate(&cte_scan)
  // Test accumulate on empty WriteTable
  acc_bool = @indCteScanAccumulate(&cte_scan)
  @indexIteratorFree(&index_iter)
  var cte = @indCteScanGetReadCte(&cte_scan)

  var ret = 0
  var tvi: TableVectorIterator
  @tableIterInit(&tvi, exec_ctx, -2147483648, temp_col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
        ret = ret + 1
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  @indCteScanFree(&cte_scan)
  return ret
}
