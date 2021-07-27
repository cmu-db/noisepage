// TODO(WAN): FIX THIS - table too big?
// Expected output: ?
// SQL: SELECT SUM(colA) from test_1;

struct State {
  sum: IntegerSumAggregate
  count: int32
}

struct Values {
  sum: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
  state.count = 0
}

fun tearDownState(state: *State) -> nil {
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var tvi: TableVectorIterator

  var table_oid : uint32
  table_oid = @testCatalogLookup(execCtx, "test_1", "")
  var col_oids: [1]uint32
  col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")

  @tableIterInit(&tvi, execCtx, table_oid, col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
      var values : Values
      values.sum = @vpiGetInt(vec, 0)
      @aggAdvance(&state.sum, &values.sum)
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var out = @ptrCast(*Values, @resultBufferAllocRow(execCtx))
  out.sum = @aggResult(&state.sum)
  for (var i : int64 = 0; @sqlToBool(i < out.sum); i = i + 1) {
    state.count = state.count + 1
  }
  @resultBufferFinalize(execCtx)
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
