fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi);) {
    var vpi = @tableIterGetVPI(&tvi)
    ret = ret + @filterLt(vpi, "colA", 500)
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
