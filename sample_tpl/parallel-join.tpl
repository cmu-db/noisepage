struct State {
  jht: JoinHashTable
}

struct ThreadState_1 {
  jht: JoinHashTable
  filter: FilterManager
}

struct BuildRow {
  key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.jht)
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
  // Filter
  @filterManagerInit(&state.filter)
  @filterManagerInsertFilter(&state.filter, _1_Lt500, _1_Lt500_Vec)
  @filterManagerFinalize(&state.filter)
  // Join hash table
  @joinHTInit(&state.jht, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
}

fun _1_pipelineWorker_TearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @filterManagerFree(&state.filter)
  @joinHTFree(&state.jht)
}

fun _1_pipelineWorker(ctx: *ExecutionContext, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var filter = &state.filter
  var jht = &state.jht
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetVPI(tvi)
    // Filter
    @filtersRun(filter, vec)
    // Insert into JHT
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var key = @vpiGetInt(vec, 0)
      var elem: *BuildRow = @joinHTInsert(jht, @hash(key))
      elem.key = key
    }
    @vpiResetFiltered(vec)
  }
  return
}

fun main(execCtx: *ExecutionContext) -> int {
  var state: State
  setUpState(execCtx, &state)

  // ---- Pipeline 1 Begin ----//

  // Setup thread state container
  var tls: ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState_1), _1_pipelineWorker_InitThreadState, _1_pipelineWorker_TearDownThreadState, execCtx)

  // Parallel scan
  @iterateTableParallel("test_1", execCtx, &tls, _1_pipelineWorker)

  // ---- Pipeline 1 End ---- //
  var off: uint32 = 0
  @joinHTBuildParallel(&state.jht, &tls, off)

  // ---- Pipeline 2 Begin ---- //

  // ---- Pipeline 2 End ---- //

  // Cleanup
  @tlsFree(&tls)
  tearDownState(&state)

  return 0
}
