struct State {
  table: JoinHashTable
  tvi1_1 : TableVectorIterator
  tvi1_2 : TableVectorIterator
  num_matches: int64
}

struct BuildRow {
  key: Integer
  val: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Init Table 1
  @tableIterConstructBind(&state.tvi1_1, "test_1", execCtx, "t1_1")
  @tableIterAddColBind(&state.tvi1_1, "t1_1", "colA")
  @tableIterAddColBind(&state.tvi1_1, "t1_1", "colB")
  @tableIterPerformInitBind(&state.tvi1_1, "t1_1")

  // Init Table 2
  @tableIterConstructBind(&state.tvi1_2, "test_1", execCtx, "t1_2")
  @tableIterAddColBind(&state.tvi1_2, "t1_2", "colA")
  @tableIterAddColBind(&state.tvi1_2, "t1_2", "colB")
  @tableIterPerformInitBind(&state.tvi1_2, "t1_2")

  @joinHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
  @tableIterClose(&state.tvi1_1)
  @tableIterClose(&state.tvi1_2)
}

fun checkKey(execCtx: *ExecutionContext, vec: *ProjectedColumnsIterator, tuple: *BuildRow) -> bool {
  if (@pciGetBind(vec, "t1_2", "colB") == tuple.key) {
    return true
  }
  return false
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  var tvi = &state.tvi1_1
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetPCI(tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(vec, "t1_1", "colA") < 1000) {
        var hash_val = @hash(@pciGetBind(vec, "t1_1", "colB"))
        var elem : *BuildRow = @ptrCast(*BuildRow, @joinHTInsert(jht, hash_val))
        elem.key = @pciGetBind(vec, "t1_1", "colB")
        elem.val = @pciGetBind(vec, "t1_1", "colA")
      }
    }
  }
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var build_row: *BuildRow
  var tvi = &state.tvi1_2
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetPCI(tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(vec, "t1_2", "colA") < 1000) {
        var hash_val = @hash(@pciGetBind(vec, "t1_2", "colB"))

        // Iterate through matches.
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.table, hash_val); @joinHTIterHasNext(&hti, checkKey, execCtx, vec); ) {
          build_row = @ptrCast(*BuildRow, @joinHTIterGetRow(&hti))
          state.num_matches = state.num_matches + 1
        }
        @joinHTIterClose(&hti)
      }
    }
  }
}


fun main(execCtx: *ExecutionContext) -> int64 {
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
