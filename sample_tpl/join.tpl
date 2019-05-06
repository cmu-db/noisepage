struct State {
  alloc: RegionAlloc
  table: JoinHashTable
}

struct BuildRow {
  key: int32
}

fun initState(state: *State) -> nil {
  // Initialize the region
  @regionInit(&state.alloc)
  // Initialize the join hash table
  @joinHTInit(&state.table, &state.alloc, @sizeOf(BuildRow))
}

fun cleanupState(state: *State) -> nil {
  // Cleanup the join hash table
  @joinHTFree(&state.table)
  // Cleanup the region allocator
  @regionFree(&state.alloc)
}

fun pipeline_1(state: *State) -> nil {
  var jht: *JoinHashTable = &state.table
  for (vec in test_1@[batch=2048]) {
    var elem: *BuildRow = @joinHTInsert(jht, 10)
    elem.key = 44
  }
}

fun pipeline_2(state: *State) -> nil {
  for (vec in test_1@[batch=1024]) { }
}

fun main() -> int32 {
  var state: State

  // Initialize state
  initState(&state)

  // Run pipeline 1
  pipeline_1(&state)

  // Build table
  @joinHTBuild(&state.table)
 
  // Run the second pipeline
  pipeline_2(&state)

  // Cleanup
  cleanupState(&state)

  return 0
}
