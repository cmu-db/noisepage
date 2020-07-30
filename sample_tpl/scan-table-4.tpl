// TODO(WAN): figure out expected output
// Expected output: wait this is a non-deterministic test?
// SQL: SELECT col1, col2 FROM test_2 WHERE (col1 < col2)

fun main(execCtx: *ExecutionContext) -> int {
  var ret = 0
  var tvi: TableVectorIterator
  var table_oid = @testCatalogLookup(execCtx, "test_2", "")
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_2", "col1")
  col_oids[1] = @testCatalogLookup(execCtx, "test_2", "col2")
  for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var col1 = @vpiGetSmallInt(vpi, 1)
      var col2 = @vpiGetInt(vpi, 0)
      if (col1 < col2) {
        ret = ret + 1
      }
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  return ret
}
