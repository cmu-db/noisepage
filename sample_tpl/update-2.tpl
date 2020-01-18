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
      var pci = @tableIterGetPCI(&tvi)
      for (; @pciHasNext(pci); @pciAdvance(pci)) {
        var cola = @pciGetInt(pci, 0)
        var colb = @pciGetInt(pci, 1)
        if (cola >= 495 and cola <= 505) {
          var slot = @pciGetSlot(pci)
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
      @pciReset(pci)
    }
  @tableIterClose(&tvi)
  @storageInterfaceFree(&updater)

  var tvi1: TableVectorIterator
  @tableIterInitBind(&tvi1, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi1)) {
    var pci1 = @tableIterGetPCI(&tvi1)
    for (; @pciHasNext(pci1); @pciAdvance(pci1)) {
      var colA = @pciGetInt(pci1, 0)
      var colB = @pciGetInt(pci1, 1)
      if (colA >= 495 and colA <= 505) {
        count = count + 1
        var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = colA
        out.col2 = colB
      }
    }
    @pciReset(pci1)
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi1)

  return count
}