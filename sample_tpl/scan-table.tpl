fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
      if (cola < 500) {
        ret = ret + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
