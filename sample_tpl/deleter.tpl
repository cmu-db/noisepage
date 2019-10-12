
struct output_struct {
  colA: Integer
  colB: Integer
}

fun index_count(execCtx: *ExecutionContext, key : int64) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  var index : IndexIterator
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  @indexIteratorInitBind(&index, execCtx, "test_1", "index_1", col_oids)

  // Next we fill up the index's projected row
  var index_pr = @indexIteratorGetPR(&index)
  @prSetInt(&index_pr, 0, @intToSql(key)) // Set colA

  // Now we iterate through the matches
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    // Materialize the current match.
    var table_pr = @indexIteratorGetTablePR(&index)

    // Read out the matching tuple to the output buffer
    var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @prGetInt(&table_pr, 0)
    out.colB = @prGetInt(&table_pr, 1)
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return count
}

fun table_count(execCtx: *ExecutionContext, oids: [*]uint32) -> int64 {
  var tvi: TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  var count : int64
  count = 0
  for (@tableIterAdvance(&tvi)) {
      var pci = @tableIterGetPCI(&tvi)
          for (; @pciHasNext(pci); @pciAdvance(pci)) {
            count = count + 1
          }
          @pciReset(pci)
    }
  @tableIterClose(&tvi)
  return count
}

fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // The following code initializes the index iterator.
  // The oids are the table col_oids that will be selected
  // Initialization

  var tvi: TableVectorIterator
  var oids: [4]uint32
  oids[0] = 1 // colA
  oids[1] = 2 // colB
  oids[2] = 3 // colC
  oids[3] = 4 // colD
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  var col0_val = 15
  var inserter : Inserter
  var deleter : Deleter
  @inserterInitBind(&inserter, execCtx, "test_1")
  @deleterInitBind(&deleter, execCtx, "test_1")
  var table_pr : *ProjectedRow = @inserterGetTablePR(&inserter)
  @prSetInt(table_pr, 0, @intToSql(col0_val))
  @prSetInt(table_pr, 1, @intToSql(14))
  @prSetInt(table_pr, 2, @intToSql(0))
  @prSetInt(table_pr, 3, @intToSql(48))

  var table_count_before_insert = table_count(execCtx, &oids)

  var ts : TupleSlot = @inserterTableInsert(&inserter)
  var index_pr : *ProjectedRow = @inserterGetIndexPRBind(&inserter, "index_1")
  @prSetInt(index_pr, 0, @prGetInt(table_pr, 0))

  var index_count_before_insert = index_count(execCtx, col0_val)
  @inserterIndexInsertBind(&inserter, "index_1")

  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  var table_count_after_insert = table_count(execCtx, &oids)
  var table_count_before_delete = table_count_after_insert
  var index_count_after_insert = index_count(execCtx, col0_val)
  var index_count_before_delete = index_count_after_insert

  @deleterTableDelete(&deleter, &ts)

  var table_count_after_delete = table_count(execCtx, &oids)
  var index_count_after_delete = index_count(execCtx, col0_val)
  return (table_count_after_delete - table_count_before_delete) + (table_count_after_insert - table_count_before_insert) + (index_count_after_insert - index_count_before_insert) + (index_count_after_delete - index_count_before_delete)
}
