struct ThreadState_1 {
  filter: FilterManager
}

fun _1_Lt500(vpi: *VectorProjectionIterator) -> int32 {
  var param: Integer = @intToSql(500)
  var cola: Integer
  if (@vpiIsFiltered(vpi)) {
    for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  } else {
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  }
  @vpiResetFiltered(vpi)
  return 0
}

fun _1_Lt500_Vec(vpi: *VectorProjectionIterator) -> int32 {
  return @filterLt(vpi, "colA", 500)
}

fun _1_pipelineWorker_InitThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerInit(&state.filter)
  @filterManagerInsertFilter(&state.filter, _1_Lt500, _1_Lt500_Vec)
  @filterManagerFinalize(&state.filter)
}

fun _1_pipelineWorker_TearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerFree(&state.filter)
}

fun _1_pipelineWorker(ctx: *ExecutionContext, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var filter = &state.filter
  for (@tableIterAdvance(tvi)) {
    var vpi = @tableIterGetVPI(tvi)
    @filtersRun(filter, vpi)
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
  @iterateTableParallel("test_1", execCtx, &tls, _1_pipelineWorker)

  // Pipeline 2

  // Cleanup
  @tlsFree(&tls)

  return 0
}
