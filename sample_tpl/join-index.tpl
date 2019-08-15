// Perform an index nested loop join for the queury:
// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA=test_2.col1 AND test_1.colB=test_2.col2
// The return value is non-deterministic. It is expected to be 900 (actually 894 the tested machine). This is because
// 10% of the potential 1000 columns contain NULLs.
// There should also be an std out output where the columns are equal

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

fun pipeline0(state : *State, execCtx : *ExecutionContext) -> nil {
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB

  var tvi : TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)

  var index : IndexIterator
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2_multi", col_oids)

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
        out.test2_col1 = @indexIteratorGetSmallInt(&index, 0)
        out.test2_col2 = @indexIteratorGetIntNull(&index, 1)
        state.count = state.count + 1
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