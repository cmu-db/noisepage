// Perform parallel scan

struct State {
    execCtx : *ExecutionContext
    count : uint32
}

struct ThreadState_1 {
    filter : FilterManager
    count  : uint32
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.execCtx = execCtx
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil { }

fun pipeline1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(execCtx, vector_proj, 0, @intToSql(500), tids)
}

fun pipeline1_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    @filterManagerInit(&state.filter, execCtx)
    @filterManagerInsertFilter(&state.filter, pipeline1_filter_clause0term0)

    state.count = 0
}

fun pipeline1_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    @filterManagerFree(&state.filter)
}

fun pipeline1_finalize(qs: *State, ts: *ThreadState_1) -> nil {
    qs.count = qs.count + ts.count
}

fun pipeline1_worker(query_state: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
    var filter = &state.filter
    for (@tableIterAdvance(tvi)) {
        var vpi = @tableIterGetVPI(tvi)

        // Filter
        @filterManagerRunFilters(filter, vpi, query_state.execCtx)

        // Count
        for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
            state.count = state.count + 1
        }
    }
    return
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // First the thread state container
    var tls = @execCtxGetTLS(execCtx)
    @tlsReset(tls, @sizeOf(ThreadState_1), pipeline1_worker_initThreadState, pipeline1_worker_tearDownThreadState, execCtx)

    // Now scan
    var table_oid : uint32
    table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [1]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    @iterateTableParallel(table_oid, col_oids, state, execCtx, 0, pipeline1_worker)

    // Collect results
    @tlsIterate(tls, state, pipeline1_finalize)

    // Cleanup
    @tlsClear(tls)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    var ret = state.count
    tearDownState(execCtx, &state)

    return ret
}
