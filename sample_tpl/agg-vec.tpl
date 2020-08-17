// TODO(WAN): this doesn't work on prashanth's branch either, needs updating?

struct State {
    table: AggregationHashTable
    count: int32
}

struct Agg {
    key: Integer
    count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.table, execCtx, @execCtxGetMem(execCtx), @sizeOf(Agg))
    state.count = 0
}

fun tearDownState(state: *State) -> nil {
    @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, vpi: *VectorProjectionIterator) -> bool {
    var key = @vpiGetInt(vpi, 1)
    return @sqlToBool(key == agg.key)
}

fun hashFn(vpi: *VectorProjectionIterator) -> uint64 {
    return @hash(@vpiGetInt(vpi, 1))
}

fun vecHashFnFiltered(hashes: [*]uint64, vec: *VectorProjectionIterator) -> nil {
    for (var idx = 0; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
        hashes[idx] = @hash(@vpiGetInt(vec, 1))
        idx = idx + 1
    }
}

fun vecHashFnUnfiltered(hashes: [*]uint64, vec: *VectorProjectionIterator) -> nil {
    for (var idx = 0; @vpiHasNext(vec); @vpiAdvance(vec)) {
        hashes[idx] = @hash(@vpiGetInt(vec, 1))
        idx = idx + 1
    }
}

fun vecHashFn(hashes: [*]uint64, vec: *VectorProjectionIterator) -> nil {
    if (@vpiIsFiltered(vec)) {
        vecHashFnFiltered(hashes, vec)
    } else {
        vecHashFnUnfiltered(hashes, vec)
    }
}

fun constructAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    // Set key
    agg.key = @vpiGetInt(vpi, 1)
    // Initialize aggregate
    @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    var input = @vpiGetInt(vpi, 0)
    @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
    // The table
    var ht: *AggregationHashTable = &state.table

    // Setup the iterator and iterate
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [2]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
    col_oids[2] = @testCatalogLookup(execCtx, "test_1", "colC")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetVPI(&tvi)
        @aggHTProcessBatch(ht, vec, hashFn, keyCheck, constructAgg, updateAgg, false)
    }
    @tableIterClose(&tvi)
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

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    // Initialize state
    setUpState(execCtx, &state)

    // Run pipeline 1
    pipeline_1(execCtx, &state)

    // Run pipeline 2
    pipeline_2(&state)

    var ret = state.count

    // Cleanup
    tearDownState(&state)

    return ret
}
