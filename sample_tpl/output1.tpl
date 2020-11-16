// Expected output: ???
// SQL: SELECT col1, col2 from test_2 WHERE col1 < 500

struct output_struct {
  col1: Integer
  col2: Integer
}

fun main(execCtx: *ExecutionContext) -> int {
  var output_buffer = @resultBufferNew(execCtx)
  var count = 0
  var out : *output_struct
  var tvi: TableVectorIterator
  var table_oid = @testCatalogLookup(execCtx, "test_2", "")
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_2", "col1")
  col_oids[1] = @testCatalogLookup(execCtx, "test_2", "col2")
  @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      if (@vpiGetSmallInt(vpi, 0) < 500) {
        out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        out.col1 = @vpiGetSmallInt(vpi, 0)
        out.col2 = @vpiGetInt(vpi, 1)
        count = count + 1
      }
    }
  }
  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  @tableIterClose(&tvi)
  return count
}
