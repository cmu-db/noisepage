// Perform:
//
// SELECT cola, colb, colc FROM test_2 WHERE (col1 < col2)
//
// Should return 5, given default random seed (number of output rows)


fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_2", execCtx)
  @tableIterAddCol(&tvi, 1) // col1
  @tableIterAddCol(&tvi, 2) // col2
  @tableIterPerformInit(&tvi)
  for (; @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var col1 = @pciGetSmallInt(pci, 1)
      var col2 = @pciGetIntNull(pci, 0)
      if (col1 < col2) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
