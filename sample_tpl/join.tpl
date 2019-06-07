struct State {
  table: JoinHashTable
  num_matches: int32
}

struct BuildRow {
  key: Integer
  val: Integer
}

struct ProbeRow {
  key: Integer
  val: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
}

fun checkKey(execCtx: *ExecutionContext, probe: *ProbeRow, tuple: *BuildRow) -> bool {
  return @sqlToBool(probe.key == tuple.key)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetInt(vec, 0) < 500) {
        var val = @pciGetInt(vec, 1)
        var hash_val = @hash(val)
        var elem : *BuildRow = @ptrCast(*BuildRow, @joinHTInsert(jht, hash_val))
        elem.key = val
        elem.val = @pciGetInt(vec, 0)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var probe_row: ProbeRow
  var build_row: *BuildRow
  var hti: JoinHashTableIterator
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetInt(vec, 0) < 500) {
        var val = @pciGetInt(vec, 1)
        probe_row.key = val
        probe_row.val = @pciGetInt(vec, 0)
        var hash_val = @hash(val)

        // Iterate through matches.
        for (@joinHTIterInit(&hti, &state.table, hash_val); @joinHTIterHasNext(&hti, checkKey, execCtx, &probe_row); ) {
          build_row = @ptrCast(*BuildRow, @joinHTIterGetRow(&hti))
          state.num_matches = state.num_matches + 1
        }
      }
    }
  }
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(execCtx, &state)

  // Build table
  @joinHTBuild(&state.table)
 
  // Run the second pipeline
  pipeline_2(execCtx, &state)

  // Cleanup
  tearDownState(&state)

  return state.num_matches
}
