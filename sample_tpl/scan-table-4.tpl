fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
      var colb = @pciGetInt(pci, 1)
      if (colb < cola) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
