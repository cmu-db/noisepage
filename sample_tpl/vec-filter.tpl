fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  oids[0] = 1 // colA
  @tableIterInitBind(&tvi, "test_1", execCtx, oids)
  for (; @tableIterAdvance(&tvi);) {
    var pci = @tableIterGetPCI(&tvi)
    ret = ret + @filterLt(pci, 0, 4, 3000)
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
