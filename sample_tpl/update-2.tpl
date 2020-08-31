// TODO(WAN): test this


// UPDATE test_1 SET colA = 100000 + colA WHERE colA BETWEEN 495 AND 505
// Returns the tuples with values BETWEEN 100495 AND 100505 (11)

struct output_struct {
  col1: Integer
  col2: Integer
}

fun main(execCtx: *ExecutionContext) -> int64 {
  var count = 0 // output count
  // Init updater
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  var updater: StorageInterface
  @storageInterfaceInitBind(&updater, execCtx, "test_1", col_oids, true)

  // Iterate through table and update
  var tvi : TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)

  for (@tableIterAdvance(&tvi)) {
      var vpi = @tableIterGetVPI(&tvi)
      for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
        var cola = @vpiGetInt(vpi, 0)
        var colb = @vpiGetInt(vpi, 1)
        if (cola >= 495 and cola <= 505) {
          var slot = @vpiGetSlot(vpi)
          // Update
          var update_pr = @getTablePR(&updater)
          @prSetInt(&update_pr, 0, cola)
          @prSetInt(&update_pr, 1, @intToSql(500))
          if (!@tableUpdate(&updater, &slot)) {
            @tableIterClose(&tvi)
            @storageInterfaceFree(&updater)
            return 37
          }
        }
      }
      @vpiReset(vpi)
    }
  @tableIterClose(&tvi)
  @storageInterfaceFree(&updater)

  var tvi1: TableVectorIterator
  @tableIterInitBind(&tvi1, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi1)) {
    var vpi1 = @tableIterGetVPI(&tvi1)
    for (; @vpiHasNext(vpi1); @vpiAdvance(vpi1)) {
      var colA = @vpiGetInt(vpi1, 0)
      var colB = @vpiGetInt(vpi1, 1)
      if (colA >= 495 and colA <= 505) {
        count = count + 1
        var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = colA
        out.col2 = colB
      }
    }
    @vpiReset(vpi1)
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi1)

  return count
}