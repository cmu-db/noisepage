fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_2", execCtx, "t2")
  @tableIterAddColBind(&tvi, "t2", "col1")
  @tableIterPerformInitBind(&tvi, "t2")
  for (; @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLtBind(pci, "t2", "col1", 500)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}