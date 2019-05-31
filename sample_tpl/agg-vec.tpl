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

fun keyCheck(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> bool {
  var key = @pciGetInt(iters[0], 0)
  return @sqlToBool(key == agg.key)
}

fun hashFn(iters: [*]*ProjectedColumnsIterator) -> uint64 {
  return @hash(@pciGetInt(iters[0], 0))
}

fun constructAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  var input = @pciGetInt(iters[0], 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var iters: [1]*ProjectedColumnsIterator

  // The table
  var ht: *AggregationHashTable = &state.table

  // Setup the iterator and iterate
  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
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
  pipeline_1(execCtx, &state)

  // Cleanup
  tearDownState(&state)

  return 0
}
