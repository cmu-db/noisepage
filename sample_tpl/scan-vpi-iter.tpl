// Expected output: 500 (number of output rows)
// SQL: SELECT * FROM test_1 WHERE cola < 500

fun filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(execCtx, vector_proj, 0, @intToSql(500), tids)
}

fun count(vpi: *VectorProjectionIterator) -> int32 {
    var ret = 0
    if (@vpiIsFiltered(vpi)) {
        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
          ret = ret + 1
        }
    } else {
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
          ret = ret + 1
        }
    }
    @vpiResetFiltered(vpi)
    return ret
}

fun main(execCtx: *ExecutionContext) -> int {
    var ret : int = 0

    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, filter_clause0term0)

    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [1]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        @filterManagerRunFilters(&filter, vpi, execCtx)
        ret = ret + count(vpi)
    }

    @filterManagerFree(&filter)
    @tableIterClose(&tvi)
    return ret
}
