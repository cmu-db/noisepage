// Perform:
// select colA from test_1 WHERE colA < 500;
//
// Should output 500 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int {
  var ret: int = 0
  var tvi: TableVectorIterator

  var table_oid : int32
  table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
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
