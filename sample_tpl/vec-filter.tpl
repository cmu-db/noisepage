fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLt(pci, "colA", 500)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
