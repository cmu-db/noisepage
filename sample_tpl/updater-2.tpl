// TODO(WAN): test this

fun index_count_2(execCtx: *ExecutionContext, key : int64, key2 : int64) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  var index : IndexIterator
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2_multi", col_oids)

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
  var index : IndexIterator
  var col_oids: [1]uint32
  col_oids[0] = 1 // colA
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

  var value0 = 5
  var value1 = 1
  var value2 = 4
  var value3 = 6

  var value1_changed = 7
  var value2_changed = 4

  var inserter : Inserter
  var updater : Updater

  @inserterInitBind(&inserter, execCtx, "test_2")
  @updaterInitBind(&updater, execCtx, "test_2", oids, true)

  var table_pr : *ProjectedRow = @inserterGetTablePR(&inserter)
  @prSetBigInt(table_pr, 0, @intToSql(value0))
  @prSetIntNull(table_pr, 1, @intToSql(value1))
  @prSetIntNull(table_pr, 2, @intToSql(value2))
  @prSetSmallInt(table_pr, 3, @intToSql(value3))

  var table_count_before_insert = table_count(execCtx, &oids)

  var ts : TupleSlot = @inserterTableInsert(&inserter)
  var index_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2")
  @prSetSmallInt(index_pr, 0, @prGetSmallInt(table_pr, 3))

  // index scan counts before index inserts
  var index_count_before_insert = index_count_1(execCtx, value3)
  var index_count_2_before_insert = index_count_2(execCtx, value3, value1)
  if(!@inserterIndexInsert(&inserter)) {
    // Free memory and abort
    @inserterFree(&inserter)
    @updaterFree(&updater)
    return 0
  }

  var table_count_after_insert = table_count(execCtx, &oids)
  var table_count_before_update = table_count_after_insert

  var index_2_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2_multi")
  @prSetSmallInt(index_2_pr, 1, @prGetSmallInt(table_pr, 3))
  @prSetInt(index_2_pr, 0, @prGetInt(table_pr, 1))
  if(!@inserterIndexInsert(&inserter)) {
    // Free memory and abort
    @inserterFree(&inserter)
    @updaterFree(&updater)
    return 0
  }

  var index_count_after_insert = index_count_1(execCtx, value3)
  var index_count_before_update = index_count_after_insert
  var index_count_2_after_insert = index_count_2(execCtx, value3, value1)
  var index_count_2_before_update_unchanged = index_count_2_after_insert
  var index_count_2_before_update_changed = index_count_2(execCtx, value3, value1_changed)
  

  // Delete from table
  if(!@updaterTableDelete(&updater, &ts)) {
    // Free memory and abort
    @inserterFree(&inserter)
    @updaterFree(&updater)
    return 0
  }
  
  // Delete from index 1
  index_pr  = @updaterGetIndexPRBind(&updater, "index_2")
  @prSetSmallInt(index_pr, 0, @intToSql(value3))
  @updaterIndexDelete(&updater, &ts)

  // Delete from index 2
  index_2_pr  = @updaterGetIndexPRBind(&updater, "index_2_multi")
  @prSetSmallInt(index_2_pr, 1, @intToSql(value3))
  @prSetInt(index_2_pr, 0, @intToSql(value1))
  @updaterIndexDelete(&updater, &ts)

  // Create table update PR
  var update_pr : *ProjectedRow = @updaterGetTablePR(&updater)
  @prSetBigInt(update_pr, 0, @intToSql(value0))
  @prSetIntNull(update_pr, 1, @intToSql(value1_changed))
  @prSetIntNull(update_pr, 2, @intToSql(value2_changed))
  @prSetSmallInt(update_pr, 3, @intToSql(value3))
  
  // Insert into table
  var ts2 : TupleSlot = @updaterTableInsert(&updater)
  
  // Insert into index 1
  index_pr  = @updaterGetIndexPRBind(&updater, "index_2")
  @prSetSmallInt(index_pr, 0, @intToSql(value3))
  if(!@updaterIndexInsert(&updater)) {
    // Free memory and abort
    @inserterFree(&inserter)
    @updaterFree(&updater)
    return 0
  }

  // Insert into index 2
  index_2_pr  = @updaterGetIndexPRBind(&updater, "index_2_multi")
  @prSetSmallInt(index_2_pr, 1, @intToSql(value3))
  @prSetInt(index_2_pr, 0, @intToSql(value1_changed))
  if(!@updaterIndexInsert(&updater)) {
    // Free memory and abort
    @inserterFree(&inserter)
    @updaterFree(&updater)
    return 0
  }

  var table_count_after_update = table_count(execCtx, &oids)
  var index_count_after_update = index_count_1(execCtx, value3)
  var index_count_2_after_update_unchanged = index_count_2(execCtx, value3, value1)
  var index_count_2_after_update_changed = index_count_2(execCtx, value3, value1_changed)

   // Free Memory
  @inserterFree(&inserter)
  @updaterFree(&updater)

  return (table_count_after_update - table_count_before_update) 
         + (table_count_after_insert - table_count_before_insert)
         + (index_count_after_insert - index_count_before_insert)
         + (index_count_after_update - index_count_before_update)
         + (index_count_2_after_insert - index_count_2_before_insert)
         + (index_count_2_after_update_changed - index_count_2_before_update_changed)
         + (index_count_2_after_update_unchanged - index_count_2_before_update_unchanged)
}
