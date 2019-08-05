fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_1", execCtx, "t1")
  @tableIterAddColBind(&tvi, "t1", "colA")
  @tableIterAddColBind(&tvi, "t1", "colB")
  @tableIterAddColBind(&tvi, "t1", "colC")
  @tableIterPerformInitBind(&tvi, "t1")
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetBind(pci, "t1", "colA")
      var colb = @pciGetBind(pci, "t1", "colB")
      var colc = @pciGetBind(pci, "t1", "colC")
      if ((cola >= 50 and colb < 10000000) or (colc < 500000)) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
