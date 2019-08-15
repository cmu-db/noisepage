// Perform in vectorized fashion:
//
// SELECT col_b, count(col_a) FROM test_1 WHERE col_a < 5000 GROUP BY col_b
//
// Should output 10 (number of distinct col_b)

struct State {
  table: AggregationHashTable
  count: int32
}

struct Agg {
  key: Integer
  cs : CountStarAggregate
  c  : CountAggregate
  sum: IntegerSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
  state.count = 0
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
  var col_oids : [2]uint32
  col_oids[0] = 1
  col_oids[1] = 2
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    @filterLt(vec, 0, 4, 5000)
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
