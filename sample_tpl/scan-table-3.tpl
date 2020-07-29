// TODO(WAN): figure out expected output
// Expected output: ???
// SQL: SELECT colA, colB, colC FROM test_1 WHERE (colA >= 50 AND colB < 10000000) OR (colC < 500000)

fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [3]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
    col_oids[2] = @testCatalogLookup(execCtx, "test_1", "colC")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var cola = @vpiGetInt(vpi, 0)
            var colb = @vpiGetInt(vpi, 1)
            var colc = @vpiGetInt(vpi, 2)
            if ((cola >= 50 and colb < 10000000) or (colc < 500000)) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
