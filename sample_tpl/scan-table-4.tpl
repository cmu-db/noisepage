fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "test_2", execCtx)
  // Read col1 only.
  @tableIterAddColBind(&tvi, "test_ns", "test_2", "col1")
  @tableIterPerformInit(&tvi)
  for (; @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // NOTE: Because we are only scanning col1, its index in the storage layer should be 0 instead of 3
      var cola = @pciGetSmallInt(pci, 0)
      if (cola < 50) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
