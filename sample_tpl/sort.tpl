// Perform
//
// SELECT colA, colB FROM test_1 WHERE colA < 2000 ORDER BY colA
//
// Should return 2000 (number of output tuples).

struct State {
  sorter: Sorter
}

struct Row {
  a: Integer 
  b: Integer
}

fun compareFn(lhs: *Row, rhs: *Row) -> int32 {
  if (lhs.a < rhs.a) {
    return -1
  } else {
    return 1
  }
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(Row))
}

fun tearDownState(state: *State) -> nil {
  @sorterFree(&state.sorter)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var sorter = &state.sorter
  var tvi: TableVectorIterator
  var oids: [2]uint32
  oids[0] = 1 // colA
  oids[1] = 2 // colB
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    @filterLt(pci, 0, 4, 2000)
    for (; @pciHasNextFiltered(pci); @pciAdvanceFiltered(pci)) {
      var row = @ptrCast(*Row, @sorterInsert(sorter))
      row.a = @pciGetInt(pci, 0)
      row.b = @pciGetInt(pci, 1)
    }
    @pciResetFiltered(pci)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> int32 {
  var ret = 0
  var sort_iter: SorterIterator
  for (@sorterIterInit(&sort_iter, &state.sorter);
       @sorterIterHasNext(&sort_iter);
       @sorterIterNext(&sort_iter)) {
    var row = @ptrCast(*Row, @sorterIterGetRow(&sort_iter))
    ret = ret + 1
  }
  @sorterIterClose(&sort_iter)
  return ret
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize
  setUpState(execCtx, &state)

  // Pipeline 1
  pipeline_1(execCtx, &state)

  // Pipeline 1 end
  @sorterSort(&state.sorter)

  // Pipeline 2
  var ret = pipeline_2(&state)

  // Cleanup
  tearDownState(&state)

  return ret
}
