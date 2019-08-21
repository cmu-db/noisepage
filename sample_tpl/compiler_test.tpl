// Perform:
// select colA from test_1 WHERE colA < 500;
//
// Should output 500 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi1: TableVectorIterator
  var tvi2: TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // col1
  oids[1] = 2 // col2
  @tableIterInitBind(&tvi1, execCtx, "test_1", oids)
  @tableIterInitBind(&tvi2, execCtx, "test_2", oids)
  for (@tableIterAdvance(&tvi2)) {
    var pci2 = @tableIterGetPCI(&tvi2)
    for (; @pciHasNext(pci2); @pciAdvance(pci2)) {

      for (@tableIterAdvance(&tvi1)) {
        var pci1 = @tableIterGetPCI(&tvi1)
        for (; @pciHasNext(pci1); @pciAdvance(pci1)) {
          ret = ret + 1
        }
      }
      @tableIterReset(&tvi1)
    }
  }
  @tableIterClose(&tvi1)
  @tableIterClose(&tvi2)
  return ret
}


/// Example Code 1 (Insert something in table, and in indexes)
// Initialize table and index
var table : SqlTable
@sqlTableInit(&table, table_oid)
var index : Index
@indexInit(&index, index_oid)

// Initialize table and index projected rows
var table_redo : *RedoRecord = @sqlTableInitRedo(&table)
var table_pr : *ProjectedRow = @redoGetPR(table_redo) // calls redo->Delta()
var index_pr : *ProjectedRow = @indexInitPR(&index)

// Set table projected row and insert
@prSetInt(table_pr, 0, @intToSql(0))
@prSetVarlen(table_pr, 1, @stringToSql("tttttt"))
var slot: *TupleSlot = @sqlTableInsert(table_redo)

// Set index projected row from table project row
@prSetInt(index_pr, 0, @prGetInt(table_pr))
@indexInsert(&index, slot, index_pr)



/// Example Code 2 (scan index, perform operations on returned tuple slots):
// Initialize the projected rows
var index_pr : *ProjectedRow = @indexInitPR(&index)
var table_pr : *ProjectedRow = @sqlTableInitPR(&table) // Maybe specify col oids

// Set the index key
@prSetInt(index_pr, 0, @intToSql(0))
@prSetVarlen(index_pr, 1, @stringToSql("tttttt"))
// Scan the index
@indexScan(&index, index_pr)

// Iterate through matches and select or delete them
for (var i = 0; i < @indexNumMatches(&index); i++) {
  var slot : *TupleSlot = @indexGetMatch(&index, i)
  // To Delete
  @sqlTableDelete(&table, slot)
  // To Select
  @sqlTableSelect(&table, slot, table_pr)
}







