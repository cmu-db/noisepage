struct State {
  table: AggregationHashTable
}

struct Agg {
  key: Integer
  cs : CountStarAggregate
  c  : CountAggregate
  sum: IntegerSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> bool {
  var key = @pciGetInt(iters[0], 1)
  return @sqlToBool(key == agg.key)
}

fun hashFn(iters: [*]*ProjectedColumnsIterator) -> uint64 {
  return @hash(@pciGetInt(iters[0], 1))
}

fun constructAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  agg.key = @pciGetInt(iters[0], 1)
  @aggInit(&agg.cs, &agg.c, &agg.sum)
}

fun updateAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  var input = @pciGetInt(iters[0], 1)
  @aggAdvance(&agg.c, &input)
  @aggAdvance(&agg.cs, &input)
  @aggAdvance(&agg.sum, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var iters: [1]*ProjectedColumnsIterator

  // The table
  var ht: *AggregationHashTable = &state.table

  // Setup the iterator and iterate
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "test_1", execCtx)
  @tableIterPerformInit(&tvi)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    @filterLt(vec, "colA", 5000)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg, false)
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
