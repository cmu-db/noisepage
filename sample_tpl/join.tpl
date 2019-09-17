// Perform using hash join:
//
// SELECT t1.col_a, t1.col_b, t1'.col_a, t1'.col_b, FROM test_1 AS t1, test_1 AS t1'
// WHERE t1.col_b = t1'.col_b AND t1.col_a < 1000 AND t1'.col_a < 1000
//
// Outputs 0 if the resulting rows match.
// TODO(Amadou): Should output number of matches once this test becomes deterministic


struct State {
  table: JoinHashTable
  num_matches: int64
  correct : bool
}

struct BuildRow {
  key: Integer
  val: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @joinHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  state.num_matches = 0
  state.correct = true
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
}

fun checkKey(execCtx: *ExecutionContext, vec: *ProjectedColumnsIterator, tuple: *BuildRow) -> bool {
  if (@pciGetInt(vec, 1) == tuple.key) {
    return true
  }
  return false
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  var tvi: TableVectorIterator
  var col_oids : [2]uint32
  col_oids[0] = 1
  col_oids[1] = 2
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetInt(vec, 0) < 1000) {
        var hash_val = @hash(@pciGetInt(vec, 1))
        var elem : *BuildRow = @ptrCast(*BuildRow, @joinHTInsert(jht, hash_val))
        elem.key = @pciGetInt(vec, 1)
        elem.val = @pciGetInt(vec, 0)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var build_row: *BuildRow
  var tvi: TableVectorIterator
  var col_oids : [2]uint32
  col_oids[0] = 1
  col_oids[1] = 2
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetInt(vec, 0) < 1000) {
        var hash_val = @hash(@pciGetInt(vec, 1))

        // Iterate through matches.
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.table, hash_val); @joinHTIterHasNext(&hti, checkKey, execCtx, vec); ) {
          build_row = @ptrCast(*BuildRow, @joinHTIterGetRow(&hti))
          state.num_matches = state.num_matches + 1
          if (build_row.key != @pciGetInt(vec, 1)) {
            state.correct = false
          }
        }
        @joinHTIterClose(&hti)
      }
    }
  }
  @tableIterClose(&tvi)
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

  // TODO(Amadou): Make this test deterministic and return the number of matches
  if (state.correct) {
    return 0
  }
  return 1 // state.num_matches
}
