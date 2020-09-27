// Expected output: 11
// SQL: SELECT colA, colB FROM test_1 WHERE colA < 2000 ORDER BY colA LIMIT 11

struct State {
    sorter : Sorter
    count  : uint32
}

struct Row {
    a: Integer
    b: Integer
}

fun compareFn(lhs: *Row, rhs: *Row) -> int32 {
    if (lhs.a < rhs.a) {
        return -1
    } else {
        return 1
    }
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @sorterInit(&state.sorter, execCtx, compareFn, @sizeOf(Row))
    state.count = 0
}

fun tearDownState(state: *State) -> nil {
    @sorterFree(&state.sorter)
}

fun pipeline1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(execCtx, vector_proj, 0, @intToSql(2000), tids)
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var sorter = &state.sorter

    // Setup filter
    var filter : FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, pipeline1_filter_clause0term0)

    var tvi: TableVectorIterator
    var test_1_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids : [2]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")

    var top_k: uint32 = 11

    for (@tableIterInit(&tvi, execCtx, test_1_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vpi = @tableIterGetVPI(&tvi)

        // Filter
        @filterManagerRunFilters(&filter, vpi, execCtx)

        // Insert into sorter
        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
            var row = @ptrCast(*Row, @sorterInsertTopK(sorter, top_k))
            row.a = @vpiGetInt(vpi, 0)
            row.b = @vpiGetInt(vpi, 1)
            @sorterInsertTopKFinish(sorter, top_k)
        }
        @vpiResetFiltered(vpi)
    }
    @tableIterClose(&tvi)

    // Sort
    @sorterSort(&state.sorter)

    // Cleanup
    @filterManagerFree(&filter)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> int32 {
    var ret = 0
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var row = @ptrCast(*Row, @sorterIterGetRow(&sort_iter))
        state.count = state.count + 1
    }
    @sorterIterClose(&sort_iter)
    return ret
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    var ret = state.count
    tearDownState(&state)

    return ret
}
