// Perform parallel join
// This will be supported after the parallel scans are added

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

fun _1_pipelineWorker(queryState: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var filter = &state.filter
  var jht = &state.jht
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetPCI(tvi)
    // Filter
    @filtersRun(filter, vec)
    // Insert into JHT
    for (; @pciHasNextFiltered(vec); @pciAdvanceFiltered(vec)) {
      var key = @pciGetInt(vec, 0)
      var elem: *BuildRow = @joinHTInsert(jht, @hash(key))
      elem.key = key
    }
    @pciResetFiltered(vec)
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
 @iterateTableParallel("test_1", &state, &tls, _1_pipelineWorker)

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
