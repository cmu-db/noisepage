// Expected output: 500 (because half of the bool_col values will be true)
// SQL:
// SELECT COUNT(*) FROM all_types_table
//  WHERE bool_col = true
//    AND (tinyint_col >= 0 OR smallint_col >= 0 OR int_col >=0 OR bigint_col >= 0)
//
// The goal of this test is to make sure that we support all of the primitive
// types in the TPL language.

fun main(execCtx: *ExecutionContext) -> int {
    var ret = 0
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "all_types_table", "")
    var col_oids: [5]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "all_types_table", "bool_col")
    col_oids[1] = @testCatalogLookup(execCtx, "all_types_table", "tinyint_col")
    col_oids[2] = @testCatalogLookup(execCtx, "all_types_table", "smallint_col")
    col_oids[3] = @testCatalogLookup(execCtx, "all_types_table", "int_col")
    col_oids[4] = @testCatalogLookup(execCtx, "all_types_table", "bigint_col")

    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
            var a = @vpiGetBool(vpi, 0)
            var b = @vpiGetTinyInt(vpi, 1)
            var c = @vpiGetSmallInt(vpi, 2)
            var d = @vpiGetInt(vpi, 3)
            var e = @vpiGetBigInt(vpi, 4)
            // var f = @vpiGetReal(vpi, 5)
            // var g = @vpiGetDouble(vpi, 6)
            if ((a == @boolToSql(true))
                and ((b >= 0) or (c >= 0) or (d >= 0) or (e >= 0))) {
                ret = ret + 1
            }
        }
        @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
    return ret
}
