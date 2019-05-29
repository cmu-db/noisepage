struct State {
  table: JoinHashTable
}

struct BuildRow {
  key: int32
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
}

fun pipeline_1(state: *State) -> nil {
  var jht: *JoinHashTable = &state.table

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)

    var hash_val = @hash(@vpiGetInt(vec, 0))
    var elem: *BuildRow = @joinHTInsert(jht, hash_val)
    elem.key = 44

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  @joinHTBuild(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  // Cleanup
  tearDownState(&state)

  return 0
}
