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
  @tableIterConstructBind(&tvi, "types1", execCtx, "t")
  @tableIterAddColBind(&tvi, "t", "int_col")
  @tableIterAddColBind(&tvi, "t", "real_col")
  @tableIterAddColBind(&tvi, "t", "date_col")
  @tableIterAddColBind(&tvi, "t", "varchar_col")
  @tableIterPerformInitBind(&tvi, "t")
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
      out.col1 = @pciGetBind(pci, "t", "int_col")
      out.col2 = @pciGetBind(pci, "t", "real_col")
      out.col3 = @pciGetBind(pci, "t", "date_col")
      out.col4 = @pciGetBind(pci, "t", "varchar_col")
      @outputAdvance(execCtx)
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 37
}