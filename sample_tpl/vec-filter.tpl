// Perform (in vectorized fashion)
//
// SELECT colA FROM test_1 WHERE colA < 3000
//
// Should return 3000 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  oids[0] = 1 // colA
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  for (; @tableIterAdvance(&tvi);) {
    var vpi = @tableIterGetVPI(&tvi)
    ret = ret + @filterLt(vpi, 0, 4, 3000)
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
