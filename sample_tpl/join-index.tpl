// This file performs index nested loop join of two tables

struct Output {
  test1_colA: Integer
  test1_colB: Integer
  test2_col1: Integer
  test2_col2: Integer
}


struct State {
  count : int64 // Debug
}

fun setupState(state : *State, execCtx : *ExecutionContext) -> nil {
  state.count = 0
}

// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA=test_2.col1 AND test_1.colB=test_2.col2
// The two columns outputted should be the same
fun pipeline0(state : *State, execCtx : *ExecutionContext) -> nil {
  var tvi : TableVectorIterator
  @tableIterConstructBind(&tvi, "test_1", execCtx, "t1")
  @tableIterAddColBind(&tvi, "t1", "colA")
  @tableIterAddColBind(&tvi, "t1", "colB")
  @tableIterPerformInitBind(&tvi, "t1")

  var index : IndexIterator
  @indexIteratorConstructBind(&index, "test_2", "index_2_multi", execCtx, "t2")
  @indexIteratorAddColBind(&index, "t2", "col1")
  @indexIteratorAddColBind(&index, "t2", "col2")
  @indexIteratorPerformInitBind(&index, "t2")

  // Iterate
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // Note that the storage layer reorders columns in test_2
      @indexIteratorSetKeyBind(&index, "t2", "index_col1", @pciGetBind(pci, "t1", "colA"))
      @indexIteratorSetKeyBind(&index, "t2", "index_col2", @pciGetBind(pci, "t1", "colB"))
      for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
        var out = @ptrCast(*Output, @outputAlloc(execCtx))
        out.test1_colA = @pciGetBind(pci, "t1", "colA")
        out.test1_colB = @pciGetBind(pci, "t1", "colB")
        out.test2_col1 = @indexIteratorGetBind(&index, "t2", "col1")
        out.test2_col2 = @indexIteratorGetBind(&index, "t2", "col2")
        state.count = state.count + 1
        @outputAdvance(execCtx)
      }
    }
  }
  // Finalize output
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
  @indexIteratorFree(&index)
}


fun main(execCtx : *ExecutionContext) -> int64 {
  var state: State
  setupState(&state, execCtx)
  pipeline0(&state, execCtx)
  return state.count
}