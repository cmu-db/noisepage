struct output_struct {
  col1: Integer
  col2: Integer
}

// SELECT colB, colC from test_1 WHERE colA < 500
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_1", execCtx, "t1")
  @tableIterAddColBind(&tvi, "t1", "colA")
  @tableIterAddColBind(&tvi, "t1", "colB")
  @tableIterPerformInitBind(&tvi, "t1")
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetBind(pci, "t1", "colA") < 500) {
        out = @ptrCast(*output_struct, @outputAlloc(execCtx))
        out.col1 = @pciGetBind(pci, "t1", "colA")
        out.col2 = @pciGetBind(pci, "t1", "colB")
        @outputAdvance(execCtx)
      }
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 0
}