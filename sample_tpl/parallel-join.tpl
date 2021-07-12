// Expected output: 500

struct State {
    execCtx     : *ExecutionContext
    jht         : JoinHashTable
    num_matches : uint32
}

struct ThreadState_1 {
    jht            : JoinHashTable
    filter_manager : FilterManager
}

struct ThreadState_2 {
    num_matches: uint32
}

struct BuildRow {
    key: Integer
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.execCtx = execCtx

    // JoinHashTable initialization
    @joinHTInit(&state.jht, execCtx, @sizeOf(BuildRow))

    // Simple bits
    state.num_matches = 0
}

fun tearDownState(state: *State) -> nil {
    @joinHTFree(&state.jht)
}

fun pipeline1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    @filterLt(execCtx, vector_proj, 0, @intToSql(500), tids)
}

fun pipeline1_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    // Filter
    @filterManagerInit(&state.filter_manager, execCtx)
    @filterManagerInsertFilter(&state.filter_manager, pipeline1_filter_clause0term0)

    // Join hash table
    @joinHTInit(&state.jht, execCtx, @sizeOf(BuildRow))
}

fun pipeline1_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_1) -> nil {
    @filterManagerFree(&state.filter_manager)
    @joinHTFree(&state.jht)
}

fun pipeline1_worker(queryState: *State, state: *ThreadState_1, tvi: *TableVectorIterator) -> nil {
    var filter = &state.filter_manager
    var jht = &state.jht
    for (@tableIterAdvance(tvi)) {
        var vec = @tableIterGetVPI(tvi)

        // Filter on colA
        @filterManagerRunFilters(filter, vec, queryState.execCtx)

        // Insert into JHT using colA as key
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var key = @vpiGetInt(vec, 0)
            var elem = @ptrCast(*BuildRow, @joinHTInsert(jht, @hash(key)))
            elem.key = key
        }

        @vpiResetFiltered(vec)
    }
}

fun pipeline2_worker_initThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil {
    state.num_matches = 0
}

fun pipeline2_worker_tearDownThreadState(execCtx: *ExecutionContext, state: *ThreadState_2) -> nil { }

fun pipeline2_worker(queryState: *State, state: *ThreadState_2, tvi: *TableVectorIterator) -> nil {
    var jht = &queryState.jht
    for (@tableIterAdvance(tvi)) {
        var vec = @tableIterGetVPI(tvi)

        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var key = @vpiGetInt(vec, 0)
            var iter: HashTableEntryIterator
            for (@joinHTLookup(&queryState.jht, &iter, @hash(key)); @htEntryIterHasNext(&iter); ) {
                var build_row = @ptrCast(*BuildRow, @htEntryIterGetRow(&iter))
                if (build_row.key == key) {
                state.num_matches = state.num_matches + 1
                }
            }
        }

        @vpiReset(vec)
    }
}

fun pipeline2_finalize(qs: *State, ts: *ThreadState_2) -> nil {
    qs.num_matches = qs.num_matches + ts.num_matches
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls = @execCtxGetTLS(execCtx)
    @tlsReset(tls, @sizeOf(ThreadState_1), pipeline1_worker_initThreadState, pipeline1_worker_tearDownThreadState, execCtx)

    // Parallel scan "test_1"
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids : [1]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    @iterateTableParallel(table_oid, col_oids, state, execCtx, 0, pipeline1_worker)

    // Parallel build the join hash table
    var off: uint32 = 0
    @joinHTBuildParallel(&state.jht, tls, off)

    // Cleanup
    @tlsClear(tls)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls = @execCtxGetTLS(execCtx)
    @tlsReset(tls, @sizeOf(ThreadState_2), pipeline2_worker_initThreadState, pipeline2_worker_tearDownThreadState, execCtx)

    // Parallel scan "test_1" again
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids : [1]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    @iterateTableParallel(table_oid, col_oids, state, execCtx, 0, pipeline2_worker)

    // Collect results
    @tlsIterate(tls, state, pipeline2_finalize)

    // Cleanup
    @tlsClear(tls)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    var ret = state.num_matches
    tearDownState(&state)

    return ret
}
