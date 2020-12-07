// Expected output: 2000 (number of output rows)
// SQL: SELECT colA FROM test_1 WHERE colA < 2000;

fun pipeline1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(execCtx, vector_proj, 0, @intToSql(2000), tids)
}

fun main(execCtx: *ExecutionContext) -> int {
  var ret: int = 0

  var filter : FilterManager
  @filterManagerInit(&filter, execCtx)
  @filterManagerInsertFilter(&filter, pipeline1_filter_clause0term0)

  var tvi: TableVectorIterator
  var table_oid : uint32
  table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")

  for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi);) {
    // Get the current vector projection
    var vpi = @tableIterGetVPI(&tvi)

    // Filter it
    @filterManagerRunFilters(&filter, vpi, execCtx)

    // Count survivors
    ret = ret + @intCast(int32, @vpiSelectedRowCount(vpi))
  }
  @tableIterClose(&tvi)
  @filterManagerFree(&filter)

  return ret
}
