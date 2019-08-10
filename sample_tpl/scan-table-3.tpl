// Perform:
//
// SELECT cola, colb, colc FROM test_1 WHERE (colA >= 50 AND colB < 10000000)
//
// Should return 9950 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_1", execCtx)
  @tableIterAddCol(&tvi, 1) // colA
  @tableIterAddCol(&tvi, 2) // colB
  @tableIterAddCol(&tvi, 3) // colC
  @tableIterPerformInit(&tvi)
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
