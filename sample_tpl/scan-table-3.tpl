fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
      var colb = @pciGetInt(pci, 1)
      var colc = @pciGetInt(pci, 2)
      if ((cola >= 50 and colb < 10000000) or (colc < 500000)) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
