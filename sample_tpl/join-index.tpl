// Perform an index nested loop join for the queury:
// SELECT test_1.colA, test_1.colB, test_2.col1, test_2.col2 FROM test_1, test_2 WHERE test_1.colA=test_2.col1 AND test_1.colB=test_2.col2
// returns 0 if the output rows match.
// Should also print out outputs
// TODO(Amadou): Return the number of matches once this test is made deterministic.

struct Output {
  test1_colA: Integer
  test1_colB: Integer
  test2_col1: Integer
  test2_col2: Integer
  test2_col3: Integer
  test2_col4: Integer
}


struct State {
  count : int64 // Debug
  correct : bool
}

fun setupState(state : *State, execCtx : *ExecutionContext) -> nil {
  state.count = 0
  state.correct = true
}

fun pipeline0(state : *State, execCtx : *ExecutionContext) -> nil {
  // Initialize table iterator
  var col_oids1: [2]uint32
  col_oids1[0] = 1 // colA
  col_oids1[1] = 2 // colB
  var tvi : TableVectorIterator
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids1)

  // Initialize index iterator
  var col_oids2: [4]uint32
  col_oids2[0] = 1 // col1 (raw offset = 3)
  col_oids2[1] = 2 // col2 (raw offset = 1)
  col_oids2[2] = 3 // col3 (raw offset = 0)
  col_oids2[3] = 4 // col4 (raw offset = 2)
  var index : IndexIterator
  @indexIteratorInitBind(&index, execCtx, "test_2", "index_2_multi", col_oids2)

  // Iterate through table
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // Fill up the index PR using the current tuple
      // Note that the storage layer reorders columns in test_2
      var index_pr = @indexIteratorGetPR(&index)
      @prSetSmallInt(&index_pr, 1, @pciGetInt(pci, 0))
      @prSetIntNull(&index_pr, 0, @pciGetInt(pci, 1))

      // Iterate through matching tuples
      for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {

        // Materialize and output
        var table_pr = @indexIteratorGetTablePR(&index)
        var out = @ptrCast(*Output, @outputAlloc(execCtx))
        out.test1_colA = @pciGetInt(pci, 0)
        out.test1_colB = @pciGetInt(pci, 1)
        out.test2_col1 = @prGetSmallInt(&table_pr, 3)
        out.test2_col2 = @prGetIntNull(&table_pr, 1)
        out.test2_col3 = @prGetBigInt(&table_pr, 0)
        out.test2_col4 = @prGetIntNull(&table_pr, 2)
        if (out.test1_colA != out.test2_col1 or out.test1_colB != out.test2_col2) {
          state.correct = false
        }
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
  if (state.correct) {
    return 0
  }
  return 1
}