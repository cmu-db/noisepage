struct output_struct {
  col1: Integer
  col2: Integer
  col3: Integer
  col4: Integer
}

fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_xx", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
      out.col1 = @pciGetInt(pci, 0)
      out.col2 = @pciGetInt(pci, 1)
      out.col3 = @pciGetInt(pci, 2)
      out.col4 = @pciGetInt(pci, 3)
      @outputAdvance(execCtx)
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 0
}