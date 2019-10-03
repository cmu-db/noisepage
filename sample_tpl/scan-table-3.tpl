// Perform:
//
// SELECT cola, colb, colc FROM test_1 WHERE (colA >= 50 AND colB < 10000000)
//
// Should return 9950 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [3]uint32
  oids[0] = 1 // colA
  oids[1] = 2 // colB
  oids[2] = 3 // colC
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
      var colb = @pciGetInt(pci, 1)
      var colc = @pciGetInt(pci, 2)
      if (cola >= 50 and colb < 10000000) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
