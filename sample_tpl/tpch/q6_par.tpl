struct Output {
    out: Real
}

// -----------------------------------------------------------------------------
// Query States
// -----------------------------------------------------------------------------

struct State {
    count : int32
    sum   : RealSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil { }

// -----------------------------------------------------------------------------
// Pipeline 1 Thread States
// -----------------------------------------------------------------------------

struct P1_ThreadState {
    ts_sum   : RealSumAggregate
    filter   : FilterManager
    ts_count : int32
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_quantity
    @filterLt(execCtx, vector_proj, 4, @floatToSql(24.0), tids)
}

fun p1_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_discount
    @filterGe(execCtx, vector_proj, 6, @floatToSql(0.05), tids)
}

fun p1_filter_clause0term2(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_discount
    @filterLe(execCtx, vector_proj, 6, @floatToSql(0.07), tids)
}

fun p1_filter_clause0term3(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_shipdate
    @filterGe(execCtx, vector_proj, 10, @dateToSql(1994, 1, 1), tids)
}

fun p1_filter_clause0term4(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_shipdate
    @filterLt(execCtx, vector_proj, 10, @dateToSql(1995, 1, 1), tids)
}

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    ts.ts_count = 0
    @aggInit(&ts.ts_sum)

    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter,
                               p1_filter_clause0term0,
                               p1_filter_clause0term1,
                               p1_filter_clause0term2,
                               p1_filter_clause0term3,
                               p1_filter_clause0term4)
    @filterManagerFinalize(&ts.filter)
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @filterManagerFree(&ts.filter)
}

// -----------------------------------------------------------------------------
// Pipeline 1
// -----------------------------------------------------------------------------

fun p1_worker(state: *State, ts: *P1_ThreadState, l_tvi: *TableVectorIterator) -> nil {
  var x = 0
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetVPI(l_tvi)

    // Filter
    @filterManagerRunFilters(&ts.filter, vec, execCtx)

    // Aggregate
    for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
      var input = @vpiGetReal(vec, 5) * @vpiGetReal(vec, 6) // extendedprice * discount
      @aggAdvance(&ts.ts_sum, &input)
      ts.ts_count = ts.ts_count + 1
    }
  }
}

fun p1_mergeAggregates(qs: *State, ts: *P1_ThreadState) -> nil {
    @aggMerge(&qs.sum, &ts.ts_sum)
    qs.count = qs.count + ts.ts_count
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)

    // Scan lineitem
    @iterateTableParallel("lineitem", state, &tls, p1_worker)

    // Merge results
    @tlsIterate(&tls, state, p1_mergeAggregates)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 2
// -----------------------------------------------------------------------------

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))
    out.out = @aggResult(&state.sum)
    @resultBufferFinalize(execCtx)
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
