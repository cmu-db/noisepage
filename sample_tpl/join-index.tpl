// This file performs index nested loop join of two tables

struct Output {
  test1_colA: Integer
  test1_colB: Integer
  test2_col1: Integer
  test2_col2: Integer
}


struct State {
  tvi : TableVectorIterator
  index : IndexIterator
}

fun setupState(state : *State, execCtx : *ExecutionContext) -> nil {
  // Read only the required columns from the storage layer.
  @tableIterConstructBind(&state.tvi, "test_ns", "test_1", execCtx)
  @tableIterAddColBind(&state.tvi, "test_ns", "test_1", "colA")
  @tableIterAddColBind(&state.tvi, "test_ns", "test_1", "colB")
  @tableIterPerformInit(&state.tvi)

  @indexIteratorConstructBind(&state.index, "test_ns", "test_2", "index_2_multi", execCtx)
  @indexIteratorAddColBind(&state.index, "test_ns", "test_2", "col1")
  @indexIteratorAddColBind(&state.index, "test_ns", "test_2", "col2")
  @indexIteratorPerformInit(&state.index)
}

fun teardownState(state : *State, execCtx : *ExecutionContext) -> nil {
  @tableIterClose(&state.tvi)
  @indexIteratorFree(&state.index)
}

// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA=test_2.col1 AND test_1.colB=test_2.col2
// The two columns outputted should be the same
fun pipeline0(state : *State, execCtx : *ExecutionContext) -> nil {
  // output variable
  var out : *Output

  for (@tableIterAdvance(&state.tvi)) {
    var pci = @tableIterGetPCI(&state.tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // Note that the storage layer reorders columns in test_@
      @indexIteratorSetKeySmallInt(&state.index, 1, @pciGetInt(pci, 0))
      @indexIteratorSetKeyInt(&state.index, 0, @pciGetInt(pci, 1))
      for (@indexIteratorScanKey(&state.index); @indexIteratorAdvance(&state.index);) {
        out = @ptrCast(*Output, @outputAlloc(execCtx))
        out.test1_colA = @pciGetInt(pci, 0)
        out.test1_colB = @pciGetInt(pci, 1)
        out.test2_col1 = @indexIteratorGetSmallInt(&state.index, 1)
        out.test2_col2 = @indexIteratorGetIntNull(&state.index, 0)
        @outputAdvance(execCtx)
      }
    }
  }
  // Finalize output
  @outputFinalize(execCtx)
}


fun main(execCtx : *ExecutionContext) -> int {
  var state: State
  setupState(&state, execCtx)
  pipeline0(&state, execCtx)
  teardownState(&state, execCtx)
  return 0
}