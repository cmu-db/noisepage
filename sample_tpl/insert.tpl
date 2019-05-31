struct row_struct {
  colA: Integer
}

fun main(execCtx: *ExecutionContext) -> int {
  // Iteration 1
  var oldcnt = 0
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "empty_table", execCtx); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      oldcnt = oldcnt + 1
      var out1 = @ptrCast(*row_struct, @outputAlloc(execCtx))
      out1.colA = @pciGetInt(pci, 0)
      @outputAdvance(execCtx)
    }
  }

  // Insert two tuples
  var ins : row_struct
  ins.colA = @intToSql(15)
  @insert(1, 2, &ins, execCtx)
  ins.colA = @intToSql(721)
  @insert(1, 2, &ins, execCtx)

  // Iteration2: expect two more results
  var cnt = 0
  var tvi2: TableVectorIterator
  for (@tableIterInit(&tvi2, "empty_table", execCtx); @tableIterAdvance(&tvi2); ) {
    var pci2 = @tableIterGetPCI(&tvi2)
    for (; @pciHasNext(pci2); @pciAdvance(pci2)) {
      cnt = cnt + 1
      var out2 = @ptrCast(*row_struct, @outputAlloc(execCtx))
      out2.colA = @pciGetInt(pci2, 0)
      @outputAdvance(execCtx)
    }
  }
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  @tableIterClose(&tvi2)
  return cnt
}