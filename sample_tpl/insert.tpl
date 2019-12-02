struct output_struct {
  col1: Integer
}

// INSERT INTO empty_table SELECT colA FROM test_1 WHERE colA BETWEEN 495 AND 505
// Returns the number of tuples inserted (11)
fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  // OIDs.
  var oids: [4]uint32
  oids[0] = 1 // colA
  oids[1] = 2 // colB
  oids[2] = 3 // colC
  oids[3] = 4 // colD

  // Init inserter
  var inserter: StorageInterface
  @storageInterfaceInitBind(&inserter, execCtx, "test_1", oids, true)

  // Init TVI
  var tvi: TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)

  // Iterate
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    @filterGe(pci, 0, 4, 495)
    @filterLe(pci, 0, 4, 505)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var colA = @pciGetInt(pci, 0)
      if (colA >= 495 and colA <= 505) {
        // Insert into table
        var insert_pr = @getTablePR(&inserter)
        @prSetInt(&insert_pr, 0, @intToSql(-502))
        var insert_slot = @tableInsert(&inserter)
        ret = ret + 1
        var out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = colA
      }
    }
    @pciReset(pci)
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  @storageInterfaceFree(&inserter)
  return ret
}