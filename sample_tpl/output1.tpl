struct output_struct {
  col1: Integer
  col2: Integer
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetInt(pci, 0) < 500) {
        out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = @pciGetInt(pci, 0)
        out.col2 = @pciGetInt(pci, 1)
        @outputAdvance(execCtx)
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 0
}