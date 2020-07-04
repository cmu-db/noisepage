// TODO(WAN): this doesn't work on prashanth's branch either, needs updating?

struct State {
    table : AggregationHashTable
    count : int32
}

struct Agg {
    key : Integer
    cs  : CountStarAggregate
    c   : CountAggregate
    sum : IntegerSumAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
    state.count = 0
}

fun tearDownState(state: *State) -> nil {
    @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, vec: *VectorProjectionIterator) -> bool {
    var key = @vpiGetInt(vec, 1)
    return @sqlToBool(key == agg.key)
}

fun hashFn(vec: *VectorProjectionIterator) -> uint64 {
    return @hash(@vpiGetInt(vec, 1))
}

fun constructAgg(agg: *Agg, vec: *VectorProjectionIterator) -> nil {
    agg.key = @vpiGetInt(vec, 1)
    @aggInit(&agg.cs, &agg.c, &agg.sum)
}

fun updateAgg(agg: *Agg, vec: *VectorProjectionIterator) -> nil {
    var input = @vpiGetInt(vec, 1)
    @aggAdvance(&agg.c, &input)
    @aggAdvance(&agg.cs, &input)
    @aggAdvance(&agg.sum, &input)
}

fun pipeline1_filter_clause0term0(vp: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(vp, 0, @intToSql(5000), tids)
}

fun pipeline_1(state: *State) -> nil {
    var filter_manager: FilterManager
    @filterManagerInit(&filter_manager)
    @filterManagerInsertFilter(&filter_manager, pipeline1_filter_clause0term0)

    // The aggregation hash table
    var ht: *AggregationHashTable = &state.table

    // Setup the iterator and iterate
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1"); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetVPI(&tvi)

        // Filter
        @filterManagerRunFilters(&filter_manager, vec)

        // Aggregate
        @aggHTProcessBatch(ht, vec, hashFn, keyCheck, constructAgg, updateAgg, false)
    }

    // Cleanup
    @tableIterClose(&tvi)
    @filterManagerFree(&filter_manager)
}

fun pipeline_2(state: *State) -> nil {
    var aht_iter: AHTIterator
    var iter = &aht_iter
    for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
        var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
        state.count = state.count + 1
    }
    @aggHTIterClose(iter)
}

fun execQuery(execCtx: *ExecutionContext, qs: *State) -> nil {
    pipeline_1(qs)
    pipeline_2(qs)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(&state)

    var ret = state.count
    return ret
}
