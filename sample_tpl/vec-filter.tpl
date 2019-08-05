fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_1", execCtx, "t1")
  @tableIterAddColBind(&tvi, "t1", "colA")
  @tableIterPerformInitBind(&tvi, "t1")
  for (; @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLtBind(pci, "t1", "colA", 500)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
