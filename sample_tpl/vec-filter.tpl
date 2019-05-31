fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLt(pci, "test_1.colA", 500)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
