fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "test_1", execCtx)
  @tableIterPerformInit(&tvi)
  for (; @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLt(pci, 0, 4, 500)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
