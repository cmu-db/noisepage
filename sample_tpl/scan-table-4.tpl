fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_2", execCtx, "t2")
  // Read col1 only.
  @tableIterAddColBind(&tvi, "t2", "col1")
  @tableIterPerformInitBind(&tvi, "t2")
  for (; @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetBind(pci, "t2", "col1")
      if (cola < 50) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
