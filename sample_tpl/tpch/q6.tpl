struct Output {
  out: Real
}

struct State {
    sum   : RealSumAggregate
    count : uint32 // debug
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil { }

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

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter,
                               p1_filter_clause0term0,
                               p1_filter_clause0term1,
                               p1_filter_clause0term2,
                               p1_filter_clause0term3,
                               p1_filter_clause0term4)
    @filterManagerFinalize(&filter)

    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "lineitem"); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)

        // Filters
        @filterManagerRunFilters(&filter, vpi, execCtx)

        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
            state.count = state.count + 1
            var input = @vpiGetReal(vpi, 5) * @vpiGetReal(vpi, 6) // extendedprice * discount
            @aggAdvance(&state.sum, &input)
        }
    }
    @tableIterClose(&tvi)

    // Cleanup
    @filterManagerFree(&filter)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))
    out.out = @aggResult(&state.sum)
    @resultBufferFinalize(execCtx)
}

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
