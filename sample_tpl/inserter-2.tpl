
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


fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected

  var tvi: TableVectorIterator
  var oids: [4]uint32
  oids[0] = 1 // colA (raw offset = 3)
  oids[1] = 2 // colB (raw offset = 1)
  oids[2] = 3 // colC (raw offset = 0)
  oids[3] = 4 // colD (raw offset = 2)

  @tableIterInitBind(&tvi, execCtx, "test_2", oids)
  var count1 : int64
  var f : int64
  count1 = 0


  var inserter : Inserter
  @inserterInitBind(&inserter, execCtx, "test_2")
  var table_pr : *ProjectedRow = @inserterGetTablePR(&inserter)
  @prSetSmallInt(table_pr, 3, @intToSql(15))
  @prSetIntNull(table_pr, 1, @intToSql(14))
  @prSetBigInt(table_pr, 0, @intToSql(0))
  @prSetIntNull(table_pr, 2, @intToSql(48))

  // counting table tuples before insert
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
        for (; @pciHasNext(pci); @pciAdvance(pci)) {
          count1 = count1 + 1
        }
        @pciReset(pci)
  }
  @tableIterClose(&tvi)

  var ts : TupleSlot = @inserterTableInsert(&inserter)
  var index_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2")
  @prSetSmallInt(index_pr, 0, @prGetSmallInt(table_pr, 3))

  // index scan counts before index inserts
  var index_count_before = index_count_1(execCtx, 15)
  var index_count_before_2 = index_count_2(execCtx, 15, 14)

  if (!@inserterIndexInsert(&inserter)) {
    // Free Memory & Abort
    @inserterFree(&inserter)
    return 0
  }

  // counting table tuples after insert
  @tableIterInitBind(&tvi, execCtx, "test_2", oids)
  var count2 : int64
  count2 = 0
  for (@tableIterAdvance(&tvi)) {
      var pci = @tableIterGetPCI(&tvi)
          for (; @pciHasNext(pci); @pciAdvance(pci)) {
            count2 = count2 + 1
          }
          @pciReset(pci)
    }
  @tableIterClose(&tvi)

 var index_pr_2 : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_2_multi")
 @prSetSmallInt(index_pr_2, 1, @prGetSmallInt(table_pr, 3))
 @prSetInt(index_pr_2, 0, @prGetInt(table_pr, 1))


 if (!@inserterIndexInsert(&inserter)) {
   // Free Memory & Abort
   @inserterFree(&inserter)
   return 0
 }


 // index scan counts after index inserts
 var index_count_after = index_count_1(execCtx, 15)
 var index_count_after_2 = index_count_2(execCtx, 15, 14)

 // Free Memory
 @inserterFree(&inserter)

 return (count2 - count1) + (index_count_after - index_count_before) + (index_count_after_2 - index_count_before_2)
}