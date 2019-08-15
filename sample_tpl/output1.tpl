// Test output buffer

struct output_struct {
  col1: Integer
  col2: Integer
}

// SELECT col1, col2 from test_2 WHERE col1 < 500
fun main(execCtx: *ExecutionContext) -> int {
  var count = 0
  var out : *output_struct
  var tvi: TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // col1
  oids[1] = 2 // col2
  @tableIterInitBind(&tvi, execCtx, "test_2", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetSmallInt(pci, 1) < 500) {
        out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = @pciGetSmallInt(pci, 1)
        out.col2 = @pciGetIntNull(pci, 0)
        count = count + 1
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return count
}