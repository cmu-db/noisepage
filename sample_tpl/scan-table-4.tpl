// Perform:
//
// SELECT col1, col2 FROM test_2 WHERE (col1 < col2)
//
// Should return 5, given default random seed (number of output rows). The expected value is 5.


fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // col1
  oids[1] = 2 // col2
  @tableIterInitBind(&tvi, execCtx, "test_2", oids)
  for (; @tableIterAdvance(&tvi); ) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var col1 = @vpiGetSmallInt(vpi, 1)
      var col2 = @vpiGetIntNull(vpi, 0)
      if (col1 < col2) {
        ret = ret + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
