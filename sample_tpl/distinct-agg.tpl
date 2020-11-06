// Expected output: 45
// SQL: SELECT SUM(DISTINCT colB) from test_1;

struct State {
  sum: IntegerSumAggregate
  distinct_table: AggregationHashTable
  count: int32
}


struct Values {
  sum: Integer
}

struct DistinctEntry {
  elem: Integer
}


fun distinctKeyCheck(old: *DistinctEntry, new: *Values) -> bool {
  return @sqlToBool(old.elem == new.sum)
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
  @aggHTInit(&state.distinct_table, execCtx, @sizeOf(DistinctEntry))
  state.count = 0
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.distinct_table)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var tvi: TableVectorIterator
  var table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [2]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
  col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
  @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var values : Values
      values.sum = @vpiGetInt(vec, 1)

      // Check if the value is distinct
      var hash_val = @hash(values.sum)
      var is_distinct = @ptrCast(*DistinctEntry, @aggHTLookup(&state.distinct_table, hash_val, distinctKeyCheck, &values))
      if (is_distinct == nil) {
        // Update the sum only if the value is distinct.
        var agg = @ptrCast(*DistinctEntry, @aggHTInsert(&state.distinct_table, hash_val))
        agg.elem = values.sum
        @aggAdvance(&state.sum, &agg.elem)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var output_buffer = @resultBufferNew(execCtx)
  var out = @ptrCast(*Values, @resultBufferAllocRow(output_buffer))
  out.sum = @aggResult(&state.sum)
  for (var i : int64 = 0; @sqlToBool(i < out.sum); i = i + 1) {
    state.count = state.count + 1
  }
  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  state.count = 0

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(execCtx, &state)

  // Run pipeline 2
  pipeline_2(execCtx, &state)

  var ret = state.count

  // Cleanup
  tearDownState(&state)

  return ret
}

