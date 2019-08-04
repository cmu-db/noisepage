struct output_struct {
  col1: Integer
  col2: Integer
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "test_2", execCtx)
  @tableIterPerformInit(&tvi)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetInt(pci, 0) < 500) {
        out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = @pciGetIntNull(pci, 1)
        out.col2 = @pciGetSmallIntNull(pci, 3)
        @outputAdvance(execCtx)
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 0
}