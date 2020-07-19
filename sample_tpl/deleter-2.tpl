// TODO(WAN) port

fun index_count_2(execCtx: *ExecutionContext, key : int64, key2 : int64) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_2", "colA")
  col_oids[1] = @testCatalogLookup(execCtx, "test_2", "colB")

  var index : IndexIterator
  var test2_oid : int32
  var index2_oid : int32
  test2_oid = @testCatalogLookup(execCtx, "test_2", "")
  index2_oid = @testCatalogIndexLookup(execCtx, "index_2_multi")
  @indexIteratorInit(&index, execCtx, 2, test2_oid, index2_oid, col_oids)

  // Next we fill up the index's projected row
  var index_pr : *ProjectedRow = @indexIteratorGetPR(&index)

  @prSetSmallInt(index_pr, 1, @intToSql(key)) // Set colA
  @prSetInt(index_pr, 0, @intToSql(key2)) // Set colB

  // Now we iterate through the matches
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  return count
}

fun index_count_1(execCtx: *ExecutionContext, key : int64) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_2", "colA")

  var index : IndexIterator
  var test2_oid : int32
  var index2_oid : int32
  test2_oid = @testCatalogLookup(execCtx, "test_2", "")
  index2_oid = @testCatalogIndexLookup(execCtx, "index_2")
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2", col_oids)

  // Next we fill up the index's projected row
  var index_pr = @indexIteratorGetPR(&index)
  @prSetSmallInt(&index_pr, 0, @intToSql(key)) // Set colA

  // Now we iterate through the matches
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  return count
}

fun table_count(execCtx: *ExecutionContext, oids: *[4]uint32) -> int64 {
  var tvi: TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_2", *oids)
  var count : int64
  count = 0
  for (@tableIterAdvance(&tvi)) {
      var vpi = @tableIterGetVPI(&tvi)
          for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            count = count + 1
          }
          @vpiReset(vpi)
    }
  @tableIterClose(&tvi)
  return count
}

fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  // Initialization

  var oids: [4]uint32
  oids[0] = 1 // colA
  oids[1] = 2 // colB
  oids[2] = 3 // colC
  oids[3] = 4 // colD

  var col0_val = 0
  var inserter : Inserter
  var deleter : Deleter
  @inserterInitBind(&inserter, execCtx, "test_2")
  @deleterInitBind(&deleter, execCtx, "test_2")
  var table_pr : *ProjectedRow = @inserterGetTablePR(&inserter)
  @prSetBigInt(table_pr, 0, @intToSql(0))
  @prSetIntNull(table_pr, 1, @intToSql(14))
  @prSetIntNull(table_pr, 2, @intToSql(48))
  @prSetSmallInt(table_pr, 3, @intToSql(15))

  var col1_val = 14
  var col3_val = 15
  var table_count_before_insert = table_count(execCtx, &oids)

  var ts : TupleSlot = @inserterTableInsert(&inserter)
  var index_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2")
  @prSetSmallInt(index_pr, 0, @prGetSmallInt(table_pr, 3))

  // index scan counts before index inserts
  var index_count_before_insert = index_count_1(execCtx, col3_val)
  var index_count_2_before_insert = index_count_2(execCtx, col3_val, col1_val)
  if (!@inserterIndexInsert(&inserter)) {
    // Free memory and abort.
    @inserterFree(&inserter)
    @deleterFree(&deleter)
    return 0
  }

  var table_count_after_insert = table_count(execCtx, &oids)
  var table_count_before_delete = table_count_after_insert

  var index_2_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2_multi")
  @prSetSmallInt(index_2_pr, 1, @prGetSmallInt(table_pr, 3))
  @prSetInt(index_2_pr, 0, @prGetInt(table_pr, 1))
  if (!@inserterIndexInsert(&inserter)) {
    // Free memory and abort.
    @inserterFree(&inserter)
    @deleterFree(&deleter)
    return 0
  }

  var index_count_after_insert = index_count_1(execCtx, col3_val)
  var index_count_before_delete = index_count_after_insert
  var index_count_2_after_insert = index_count_2(execCtx, col3_val, col1_val)
  var index_count_2_before_delete = index_count_2_after_insert

  if (!@deleterTableDelete(&deleter, &ts)) {
    // Free memory and abort.
    @inserterFree(&inserter)
    @deleterFree(&deleter)
    return 0
  }

  var index_delete_pr : *ProjectedRow = @deleterGetIndexPRBind(&deleter, "index_2")
  @prSetSmallInt(index_delete_pr, 0, @prGetSmallInt(table_pr, 3))
  @deleterIndexDelete(&deleter, &ts)

  var index_2_delete_pr : *ProjectedRow = @deleterGetIndexPRBind(&deleter, "index_2_multi")
  @prSetSmallInt(index_2_delete_pr, 1, @prGetSmallInt(table_pr, 3))
  @prSetInt(index_2_delete_pr, 0, @prGetInt(table_pr, 1))
  @deleterIndexDelete(&deleter, &ts)


  var table_count_after_delete = table_count(execCtx, &oids)
  var index_count_after_delete = index_count_1(execCtx, col3_val)
  var index_count_2_after_delete = index_count_2(execCtx, col3_val, col1_val)

  // Free Memory
  @inserterFree(&inserter)
  @deleterFree(&deleter)

  return (table_count_after_delete - table_count_before_delete) + (table_count_after_insert - table_count_before_insert) + (index_count_after_insert - index_count_before_insert) + (index_count_after_delete - index_count_before_delete) +
  (index_count_2_after_insert - index_count_2_before_insert) + (index_count_2_after_delete - index_count_2_before_delete)
}
