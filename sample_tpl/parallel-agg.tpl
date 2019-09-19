// Perform parallel aggregation
// This will be supported after the parallel scans are added

struct State {
  table: AggregationHashTable
  count: int32
}

struct ThreadState_1 {
  table: AggregationHashTable
}

struct ThreadState_2 {
  count: int32
}

struct Agg {
  key: Integer
  cs : CountStarAggregate
}

fun keyCheck(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> bool {
  var key = @pciGetInt(iters[0], 1)
  return @sqlToBool(key == agg.key)
}

fun keyCheckPartial(agg1: *Agg, agg2: *Agg) -> bool {
  return @sqlToBool(agg1.key == agg2.key)
}

fun hashFn(iters: [*]*ProjectedColumnsIterator) -> uint64 {
  return @hash(@pciGetInt(iters[0], 1))
}

fun constructAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  agg.key = @pciGetInt(iters[0], 1)
  @aggInit(&agg.cs)
}

fun constructAggFromPartial(agg: *Agg, partial: *Agg) -> nil {
  agg.key = partial.key
  @aggInit(&agg.cs)
}

fun updateAgg(agg: *Agg, iters: [*]*ProjectedColumnsIterator) -> nil {
  var input = @pciGetInt(iters[0], 1)
  @aggAdvance(&agg.cs, &input)
}

fun updateAggFromPartial(agg: *Agg, partial: *Agg) -> nil {
  @aggMerge(&agg.cs, &partial.cs)
}

fun initState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun p1_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun p1_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
  @aggHTFree(&state.table)
}

fun p1_worker(queryState: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
  var iters: [1]*ProjectedColumnsIterator
  var ht: *AggregationHashTable = &state.table

  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetVPI(tvi)
    iters[0] = vec
    @aggHTProcessBatch(ht, &iters, hashFn, keyCheck, constructAgg, updateAgg)
  }
  return
}

fun p1_mergePartitions(qs: *State, table: *AggregationHashTable, iter: *AggOverflowPartIter) -> nil {
  var x = 0
  for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
    var partial_hash = @aggPartIterGetHash(iter)
    var partial = @ptrCast(*Agg, @aggPartIterGetRow(iter))
    var agg = @ptrCast(*Agg, @aggHTLookup(table, partial_hash, keyCheckPartial, partial))
    if (agg == nil) {
      agg = @ptrCast(*Agg, @aggHTInsert(table, partial_hash))
      constructAggFromPartial(agg, partial)
    } else {
      updateAggFromPartial(agg, partial)
    }
  }
}

fun p2_worker_initThreadState(execCtx: *ExecutionContext, ts: *ThreadState_2) -> nil {
  ts.count = 0
}

fun p2_worker_tearDownThreadState(execCtx: *ExecutionContext, ts: *ThreadState_1) -> nil {
}

fun p2_finalize(qs: *State, ts: *ThreadState_2) -> nil {
  qs.count = qs.count + ts.count
}

fun p2_worker(qs: *State, ts: *ThreadState_2, table: *AggregationHashTable) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var iter = &agg_ht_iter
  for (@aggHTIterInit(iter, table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
    var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
    ts.count = ts.count + 1
  }
  @aggHTIterClose(iter)
}

fun main(execCtx: *ExecutionContext) -> int {
  var state: State
  state.count = 0

  // ---- Init ---- //

  initState(execCtx, &state)

  // ---- Pipeline 1 Begin ---- // 
  
  var tls: ThreadStateContainer
  @tlsInit(&tls, @execCtxGetMem(execCtx))
  @tlsReset(&tls, @sizeOf(ThreadState_1), p1_worker_initThreadState, p1_worker_tearDownThreadState, execCtx)

  // Parallel Scan
  @iterateTableParallel("test_1", &state, &tls, p1_worker)

  // ---- Pipeline 1 End ---- // 

  // Move thread-local states
  var aht_off: uint32 = 0
  @aggHTMoveParts(&state.table, &tls, aht_off, p1_mergePartitions)

  // ---- Pipeline 2 Begin ---- //

  @tlsReset(&tls, @sizeOf(ThreadState_2), p2_worker_initThreadState, p2_worker_tearDownThreadState, execCtx)
  @aggHTParallelPartScan(&state.table, &state, &tls, p2_worker)

  // ---- Pipeline 2 End ---- //

  @tlsIterate(&tls, &state, p2_finalize)

  // ---- Clean Up ---- //

  var ret = state.count

  @tlsFree(&tls)
  tearDownState(execCtx, &state)

  return ret
}
