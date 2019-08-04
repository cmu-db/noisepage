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
  @tableIterConstructBind(&tvi, "test_ns", "test_1", execCtx)
  @tableIterAddColBind(&tvi, "test_ns", "test_1", "colA")
  @tableIterAddColBind(&tvi, "test_ns", "test_1", "colB")
  @tableIterPerformInit(&tvi)

  var index : IndexIterator
  @indexIteratorConstructBind(&index, "test_ns", "test_2", "index_2_multi", execCtx)
  @indexIteratorAddColBind(&index, "test_ns", "test_2", "col1")
  @indexIteratorAddColBind(&index, "test_ns", "test_2", "col2")
  @indexIteratorPerformInit(&index)

  // Iterate
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // Note that the storage layer reorders columns in test_2
      @indexIteratorSetKeySmallInt(&index, 1, @pciGetInt(pci, 0))
      @indexIteratorSetKeyInt(&index, 0, @pciGetInt(pci, 1))
      for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
        var out = @ptrCast(*Output, @outputAlloc(execCtx))
        out.test1_colA = @pciGetInt(pci, 0)
        out.test1_colB = @pciGetInt(pci, 1)
        out.test2_col1 = @indexIteratorGetSmallInt(&index, 1)
        out.test2_col2 = @indexIteratorGetIntNull(&index, 0)
        state.count = state.count + 1
        @outputAdvance(execCtx)
      }
    }
  }
  // Finalize output
  @outputFinalize(execCtx)
}


fun main(execCtx : *ExecutionContext) -> int64 {
  var state: State
  setupState(&state, execCtx)
  pipeline0(&state, execCtx)
  return state.count
}