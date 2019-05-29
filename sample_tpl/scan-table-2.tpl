fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var cola = @vpiGetInt(vpi, 0)
      var colb = @vpiGetInt(vpi, 1)
      if (cola >= 50 and colb < 10000000) {
        ret = ret + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
