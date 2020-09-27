// Expected output: 1000 (number of matching rows)
// SQL:
// SELECT t1.col_a, t1.col_b, t1'.col_a, t1'.col_b, FROM test_1 AS t1, test_1 AS t1'
// WHERE t1.col_b = t1'.col_b AND t1.col_a < 1000 AND t1'.col_a < 1000

struct State {
  table: JoinHashTable
  num_matches: int32
}

struct BuildRow {
  key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Complex bits
  @joinHTInit(&state.table, execCtx, @sizeOf(BuildRow))
  // Simple bits
  state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
  @joinHTFree(&state.table)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var jht: *JoinHashTable = &state.table

  var tvi: TableVectorIterator
  var table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
  for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var key = @vpiGetInt(vec, 0)
      if (key < 1000) {
        var hash_val = @hash(key)
        var elem = @ptrCast(*BuildRow, @joinHTInsert(jht, hash_val))
        elem.key = key
      }
    }

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var tvi: TableVectorIterator
  var table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
  for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)

    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var key = @vpiGetInt(vec, 0)
      if (key < 1000) {
        var hash_val = @hash(key)

        var iter: HashTableEntryIterator
        for (@joinHTLookup(&state.table, &iter, hash_val); @htEntryIterHasNext(&iter); ) {
          var build_row = @ptrCast(*BuildRow, @htEntryIterGetRow(&iter))
          if (build_row.key == key) {
            state.num_matches = state.num_matches + 1
          }
        }
      }
    }

    @vpiReset(vec)
  }
  @tableIterClose(&tvi)
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

  var ret = state.num_matches

  // Cleanup
  tearDownState(&state)

  return ret
}
