struct State {
}


struct ThreadState_1 {
  filter: FilterManager
}

fun _1_Lt500(pci: *ProjectedColumnsIterator) -> int32 {
  var param: Integer = @intToSql(500)
  var cola: Integer
  if (@pciIsFiltered(pci)) {
    for (; @pciHasNextFiltered(pci); @pciAdvanceFiltered(pci)) {
      cola = @pciGetInt(pci, 0)
      @pciMatch(pci, cola < param)
    }
  } else {
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      cola = @pciGetInt(pci, 0)
      @pciMatch(pci, cola < param)
    }
  }
  @pciResetFiltered(pci)
  return 0
}

fun _1_Lt500_Vec(pci: *ProjectedColumnsIterator) -> int32 {
  return @filterLt(pci, "colA", 500)
}

fun _1_pipelineWorker_InitThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerInit(&state.filter)
  @filterManagerInsertFilter(&state.filter, _1_Lt500, _1_Lt500_Vec)
  @filterManagerFinalize(&state.filter)
}

fun _1_pipelineWorker_TearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerFree(&state.filter)
}

fun _1_pipelineWorker(query_state: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var filter = &state.filter
  for (@tableIterAdvance(tvi)) {
    var pci = @tableIterGetPCI(tvi)
    @filtersRun(filter, pci)
  }
  return
}

fun main(execCtx: *ExecutionContext) -> int {
  // Pipeline 1 - parallel scan table

  // First the thread state container
  var tls: ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState_1), _1_pipelineWorker_InitThreadState, _1_pipelineWorker_TearDownThreadState, execCtx)

  // Now scan
  @iterateTableParallel("test_1", &state, &tls, _1_pipelineWorker)

  // Pipeline 2

  // Cleanup
  @tlsFree(&tls)

  return 0
}
