struct OutputStruct {
    ps_partkey : Integer
    value      : Real
}

struct JoinRow1 {
    n_nationkey : Integer
}

struct JoinRow2 {
    s_suppkey : Integer
}

struct AggPayload1 {
    value : RealSumAggregate
}

struct AggValues1 {
    value : Real
}

struct AggPayload2 {
    ps_partkey : Integer
    value      : RealSumAggregate
}

struct AggValues2 {
    ps_partkey : Integer
    value      : Real
}

struct SorterRow {
    ps_partkey : Integer
    value      : Real
}

struct State {
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    agg1        : RealSumAggregate
    agg_table2  : AggregationHashTable
    sorter      : Sorter
    count       : int32 // Debug
}

struct P1_ThreadState {
    ts_join_table : JoinHashTable
    filter        : FilterManager
}

struct P2_ThreadState {
    ts_join_table : JoinHashTable
}

struct P3_ThreadState_1 {
    ts_agg  : RealSumAggregate
}

struct P3_ThreadState_2 {
    ts_agg_table : AggregationHashTable
}

struct P4_ThreadState {
    ts_sorter : Sorter
}

fun p1Filter1(vec: *VectorProjectionIterator) -> int32 {
    var germany = @stringToSql("GERMANY")
    for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
        // n_name = "germany"
        @vpiMatch(vec, @vpiGetString(vec, 1) == germany)
    }
    @vpiResetFiltered(vec)
    return 0
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // check s_nationkey == n_nationkey
    if (@vpiGetInt(probe, 3) != build.n_nationkey) {
        return false
    }
    return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
    // check ps_suppkey == s_suppkey
    if (@vpiGetInt(probe, 1) != build.s_suppkey) {
        return false
    }
    return true
}

// Check that the aggregate key already exists
fun checkAggKey2(payload: *AggPayload2, row: *AggValues2) -> bool {
    if (payload.ps_partkey != row.ps_partkey) {
        return false
    }
    return true
}

fun aggKeyCheckPartial2(agg_payload1: *AggPayload2, agg_payload2: *AggPayload2) -> bool {
    return @sqlToBool(agg_payload1.ps_partkey == agg_payload2.ps_partkey)
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.value < rhs.value) {
        return -1
    }
    if (lhs.value > rhs.value) {
        return 1
    }
    return 0
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
    @aggInit(&state.agg1)
    @aggHTInit(&state.agg_table2, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
    state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @aggHTFree(&state.agg_table2)
    @sorterFree(&state.sorter)
}

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p1Filter1)
    @filterManagerFinalize(&ts.filter)
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
    @filterManagerFree(&ts.filter)
}

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

fun p3_initThreadState_1(execCtx: *ExecutionContext, ts: *P3_ThreadState_1) -> nil {
    @aggInit(&ts.ts_agg)
}

fun p3_tearDownThreadState_1(execCtx: *ExecutionContext, ts: *P3_ThreadState_1) -> nil {
}

fun p3_initThreadState_2(execCtx: *ExecutionContext, ts: *P3_ThreadState_2) -> nil {
    @aggHTInit(&ts.ts_agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
}

fun p3_tearDownThreadState_2(execCtx: *ExecutionContext, ts: *P3_ThreadState_2) -> nil {
    @aggHTFree(&ts.ts_agg_table)
}

fun p4_initThreadState(execCtx: *ExecutionContext, ts: *P4_ThreadState) -> nil {
    @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun p4_tearDownThreadState(execCtx: *ExecutionContext, ts: *P4_ThreadState) -> nil {
    @sorterFree(&ts.ts_sorter)
}

// Scan nation build JHT1
fun p1_worker(state: *State, ts: *P1_ThreadState, n_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(n_tvi)) {
        var vec = @tableIterGetVPI(n_tvi)
        @filtersRun(&ts.filter, vec)
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            // Step 2: Insert into Hash Table
            var hash_val = @hash(@vpiGetInt(vec, 0)) // n_nationkey
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row1.n_nationkey = @vpiGetInt(vec, 0) // n_nationkey
        }
    }
}

// Scan supplier, scan JHT1, build JHT2
fun p2_worker(state: *State, ts: *P2_ThreadState, s_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(s_tvi)) {
        var vec = @tableIterGetVPI(s_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Probe JHT1
            // Step 2: Probe HT1
            var hash_val = @hash(@vpiGetInt(vec, 3)) // s_nationkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, state, vec);) {
                var join_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

                // Step 3: Build HT2
                var hash_val2 = @hash(@vpiGetInt(vec, 0)) // s_suppkey
                var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val2))
                build_row2.s_suppkey = @vpiGetInt(vec, 0)
            }
        }
    }
}

// Scan partsupp, probe HT2, advance agg1
fun p3_worker_1(state: *State, ts: *P3_ThreadState_1, ps_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(ps_tvi)) {
        var vec = @tableIterGetVPI(ps_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1)) // ps_suppkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, state, vec);) {
                var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))
                var agg_input = @vpiGetReal(vec, 3) * @vpiGetInt(vec, 2)
                @aggAdvance(&ts.ts_agg, &agg_input)
            }
        }
    }
}

fun gatherAgg(qs: *State, ts: *P3_ThreadState_1) -> nil {
    @aggMerge(&qs.agg1, &ts.ts_agg)
}

// Scan partsupp, probe HT2, build agg
fun p3_worker_2(state: *State, ts: *P3_ThreadState_2, ps_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(ps_tvi)) {
        var vec = @tableIterGetVPI(ps_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1)) // ps_suppkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, state, vec);) {
                var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))
                var agg_input : AggValues2 // Materialize
                agg_input.ps_partkey = @vpiGetInt(vec, 0)
                agg_input.value = @vpiGetReal(vec, 3) * @vpiGetInt(vec, 2)
                var agg_hash_val = @hash(agg_input.ps_partkey)
                var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey2, &agg_input))
                if (agg_payload == nil) {
                    agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
                    agg_payload.ps_partkey = agg_input.ps_partkey
                    @aggInit(&agg_payload.value)
                }
                @aggAdvance(&agg_payload.value, &agg_input.value)
            }
        }
    }
}

fun mergePartitions3_2(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggPayload2, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial2, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            @aggMerge(&agg_payload.value, &partial.value)
        }
    }
}

// BNL, sort
fun p4_worker(state: *State, ts: *P4_ThreadState, agg_table: *AggregationHashTable) -> nil {
    var aht_iter: AHTIterator
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg_payload = @ptrCast(*AggPayload2, @aggHTIterGetRow(&aht_iter))
        if (@aggResult(&agg_payload.value) > (@aggResult(&state.agg1) * 0.0001)) {
            // Step 2: Build Sorter
            var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
            sorter_row.ps_partkey = agg_payload.ps_partkey
            sorter_row.value = @aggResult(&agg_payload.value)
        }
    }
    @aggHTIterClose(&aht_iter)
}

// Iterate through sorter, output
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.ps_partkey = sorter_row.ps_partkey
        out.value = sorter_row.value

        state.count = state.count + 1
    }
    @sorterIterClose(&sort_iter)

    @resultBufferFinalize(execCtx)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    // set up state
    setUpState(execCtx, &state)
    var off: uint32 = 0
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))

    // Pipeline 1
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)
    @iterateTableParallel("nation", &state, &tls, p1_worker)
    @joinHTBuildParallel(&state.join_table1, &tls, off)

    // Pipeline 2
    @tlsReset(&tls, @sizeOf(P2_ThreadState), p2_initThreadState, p2_tearDownThreadState, execCtx)
    @iterateTableParallel("supplier", &state, &tls, p2_worker)
    @joinHTBuildParallel(&state.join_table2, &tls, off)

    // Pipeline 3.1
    @tlsReset(&tls, @sizeOf(P3_ThreadState_1), p3_initThreadState_1, p3_tearDownThreadState_1, execCtx)
    @iterateTableParallel("partsupp", &state, &tls, p3_worker_1)
    @tlsIterate(&tls, &state, gatherAgg)

    // Pipeline 3.2
    @tlsReset(&tls, @sizeOf(P3_ThreadState_2), p3_initThreadState_2, p3_tearDownThreadState_2, execCtx)
    @iterateTableParallel("partsupp", &state, &tls, p3_worker_2)
    @aggHTMoveParts(&state.agg_table2, &tls, off, mergePartitions3_2)

    // Pipeline 4
    @tlsReset(&tls, @sizeOf(P4_ThreadState), p4_initThreadState, p4_tearDownThreadState, execCtx)
    @aggHTParallelPartScan(&state.agg_table2, &state, &tls, p4_worker)
    @sorterSortParallel(&state.sorter, &tls, off)

    // Pipeline 5
    pipeline5(execCtx, &state)

    // Clean up thread- and query-state
    @tlsFree(&tls)
    teardownState(execCtx, &state)

    return state.count
}
