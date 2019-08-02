struct State {
  table: AggregationHashTable
  count: int32
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
  var key = @pciGetInt(iters[0], 1)
  return @sqlToBool(key == agg.key)
}

fun hashFn(iters: [*]*ProjectedColumnsIterator) -> uint64 {
  return @hash(@pciGetInt(iters[0], 1))
}

fun constructAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  // Set key
  agg.key = @pciGetInt(iters[0], 1)
  // Initialize aggregate
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
  @tableIterConstructBind(&tvi, "test_ns", "test_1", execCtx)
  @tableIterPerformInit(&tvi)
  for (; @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg, false)
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var iter = &agg_ht_iter
  for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
    var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
    state.count = state.count + 1
  }
  @aggHTIterClose(iter)
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
