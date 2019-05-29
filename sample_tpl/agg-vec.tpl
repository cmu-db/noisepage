struct State {
  table: AggregationHashTable
}

struct Agg {
  key: Integer
  count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, iters: [*]*VectorProjectionIterator) -> bool {
  var key = @vpiGetInt(iters[0], 0)
  return @sqlToBool(key == agg.key)
}

fun hashFn(iters: [*]*VectorProjectionIterator) -> uint64 {
  return @hash(@vpiGetInt(iters[0], 0))
}

fun constructAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, iters: [*]*VectorProjectionIterator) -> nil {
  var input = @vpiGetInt(iters[0], 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(state: *State) -> nil {
  var iters: [1]*VectorProjectionIterator

  // The table
  var ht: *AggregationHashTable = &state.table

  // Setup the iterator and iterate
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetVPI(&tvi)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg)
  }
  @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(&state)

  // Cleanup
  tearDownState(&state)

  return 0
}
