// Expected output: 9950
// SQL: SELECT colA, colB FROM test_1 WHERE (colA >= 50 AND colB < 10000)

fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [2]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var cola = @vpiGetInt(vpi, 0)
            var colb = @vpiGetInt(vpi, 1)
            if (cola >= @intToSql(50) and colb < @intToSql(10000)) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
