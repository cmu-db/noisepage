fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "test_1", execCtx)
  @tableIterAddColBind(&tvi, "test_ns", "test_1", "colA")
  @tableIterPerformInit(&tvi)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
      if (cola < 500) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
