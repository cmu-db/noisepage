struct output_struct {
  col1: Integer
  col2: Real
  col3: Date
  col4: StringVal
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "types1", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
      out.col1 = @pciGetInt(pci, 2)
      out.col2 = @pciGetDouble(pci, 1)
      out.col3 = @pciGetDate(pci, 3)
      out.col4 = @pciGetVarlen(pci, 0)
      @outputAdvance(execCtx)
    }
  }
  @outputFinalize(execCtx)
  return 0
}