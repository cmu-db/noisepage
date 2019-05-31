struct output_struct {
  test1_colA: Integer
  test1_colB: Integer
  test2_col1: Integer
  test2_col2: Integer
}

// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA = test_2.col1;
fun main(execCtx: *ExecutionContext) -> int {
  var out : *output_struct
  var tvi1: TableVectorIterator
  var tvi2: TableVectorIterator
  for (@tableIterInit(&tvi1, "test_1", execCtx); @tableIterAdvance(&tvi1); ) {
      var pci1 = @tableIterGetPCI(&tvi1)
      for (; @pciHasNext(pci1); @pciAdvance(pci1)) {
          for (@tableIterInit(&tvi2, "test_2", execCtx); @tableIterAdvance(&tvi2); ) {
              var pci2 = @tableIterGetPCI(&tvi2)
              for (; @pciHasNext(pci2); @pciAdvance(pci2)) {
                  // NOTE: Due to the reordering of the columns, test_2.col1's index is 3.
                  if (@pciGetInt(pci1, 0) == @pciGetSmallInt(pci2, 3)) {
                      out = @ptrCast(*output_struct, @outputAlloc(execCtx))
                      out.test1_colA = @pciGetInt(pci1, 0)
                      out.test1_colB = @pciGetInt(pci1, 1)
                      out.test2_col1 = @pciGetSmallInt(pci2, 3)
                      out.test2_col2 = @pciGetInt(pci2, 1)
                      @outputAdvance(execCtx)
                  }
              }
          }
      }
    }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  return 42
}