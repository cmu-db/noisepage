// The code is written to (almost) match the way it will be codegened, so it may be overly verbose.
// But technically, some of these structs are redundant (like AggValues or SorterRow)

struct OutputStruct {
    n_name  : StringVal
    revenue : Real
}

struct State {
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    join_table3 : JoinHashTable
    join_table4 : JoinHashTable
    join_table5 : JoinHashTable
    agg_table   : AggregationHashTable
    sorter      : Sorter
    count       : int32 // For debugging
}

struct P1_ThreadState {
    ts_join_table : JoinHashTable
    filter        : FilterManager
    ts_count      : int32
}

struct P2_ThreadState {
    ts_join_table : JoinHashTable
    ts_count      : int32
}

struct P3_ThreadState {
    ts_join_table : JoinHashTable
    ts_count      : int32
}

struct P4_ThreadState {
    ts_join_table : JoinHashTable
    filter        : FilterManager
    ts_count      : int32
}

struct P5_ThreadState {
    ts_join_table : JoinHashTable
    ts_count      : int32
}

struct P6_ThreadState {
    ts_agg_table : AggregationHashTable
    ts_count     : int32
}

struct P7_ThreadState {
    ts_sorter : Sorter
    ts_count  : int32
}

struct JoinRow1 {
    r_regionkey : Integer
}

struct JoinRow2 {
    n_name      : StringVal
    n_nationkey : Integer
}

struct JoinRow3 {
    n_name      : StringVal
    n_nationkey : Integer
    c_custkey   : Integer
}

struct JoinRow4 {
    n_name      : StringVal
    n_nationkey : Integer
    o_orderkey  : Integer
}

struct JoinProbe5 {
    n_nationkey : Integer
    l_suppkey   : Integer
}

struct JoinRow5 {
    s_suppkey   : Integer
    s_nationkey : Integer
}

// Aggregate payload
struct AggPayload {
    n_name  : StringVal
    revenue : RealSumAggregate
}

// Input of aggregate
struct AggValues {
    n_name  : StringVal
    revenue : Real
}

// Input and Output of sorter
struct SorterRow {
    n_name  : StringVal
    revenue : Real
}

fun checkAggKeyFn(payload: *AggPayload, row: *AggValues) -> bool {
    if (payload.n_name != row.n_name) {
        return false
    }
    return true
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
    return @sqlToBool(agg_payload1.n_name == agg_payload2.n_name)
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // check n_regionkey == r_regionkey
    if (@vpiGetInt(probe, 2) != build.r_regionkey) {
        return false
    }
    return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
    // check c_nationkey == n_nationkey
    if (@vpiGetInt(probe, 3) != build.n_nationkey) {
        return false
    }
    return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow3) -> bool {
    // o_custkey == c_custkey
    if (@vpiGetInt(probe, 1) != build.c_custkey) {
        return false
    }
    return true
}

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow4) -> bool {
    // l_orderkey == o_orderkey
    if (@vpiGetInt(probe, 0) != build.o_orderkey) {
        return false
    }
    return true
}

fun checkJoinKey5(execCtx: *ExecutionContext, probe: *JoinProbe5, build: *JoinRow5) -> bool {
    if (probe.n_nationkey != build.s_nationkey) {
        return false
    }
    // l_suppkey == s_suppkey
    if (probe.l_suppkey != build.s_suppkey) {
        return false
    }
    return true
}

fun checkAggKey(payload: *AggPayload, values: *AggValues) -> bool {
    if (payload.n_name != values.n_name) {
        return false
    }
    return true
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.revenue < rhs.revenue) {
        return -1
    }
    if (lhs.revenue > rhs.revenue) {
        return 1
    }
    return 0
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // r_name
    @filterEq(execCtx, vector_proj, 1, @stringToSql("ASIA"), tids)
}

fun p4_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // o_orderdate
    @filterGe(execCtx, vector_proj, 4, @dateToSql(1990, 1, 1), tids)
}

fun p4_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // o_orderdate
    @filterLe(execCtx, vector_proj, 4, @dateToSql(2000, 1, 1), tids)
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    // Initialize hash tables
    @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
    @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
    @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
    @joinHTInit(&state.join_table5, @execCtxGetMem(execCtx), @sizeOf(JoinRow5))

    // Initialize aggregate
    @aggHTInit(&state.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))

    // Initialize Sorter
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @joinHTFree(&state.join_table3)
    @joinHTFree(&state.join_table4)
    @joinHTFree(&state.join_table5)
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
}

// -----------------------------------------------------------------------------
// Pipeline 1 Thread States
// -----------------------------------------------------------------------------

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    ts.ts_count = 0
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p1_filter_clause0term0)
    @filterManagerFinalize(&ts.filter)
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
    @filterManagerFree(&ts.filter)
}

// -----------------------------------------------------------------------------
// Pipeline 2 Thread States
// -----------------------------------------------------------------------------

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    ts.ts_count = 0
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 3 Thread States
// -----------------------------------------------------------------------------

fun p3_initThreadState(execCtx: *ExecutionContext, ts: *P3_ThreadState) -> nil {
    ts.ts_count = 0
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
}

fun p3_tearDownThreadState(execCtx: *ExecutionContext, ts: *P3_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 4 Thread States
// -----------------------------------------------------------------------------

fun p4_initThreadState(execCtx: *ExecutionContext, ts: *P4_ThreadState) -> nil {
    ts.ts_count = 0
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p4_filter_clause0term0, p4_filter_clause0term1)
    @filterManagerFinalize(&ts.filter)
}

fun p4_tearDownThreadState(execCtx: *ExecutionContext, ts: *P4_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
    @filterManagerFree(&ts.filter)
}

// -----------------------------------------------------------------------------
// Pipeline 5 Thread States
// -----------------------------------------------------------------------------

fun p5_initThreadState(execCtx: *ExecutionContext, ts: *P5_ThreadState) -> nil {
    ts.ts_count = 0
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow5))
}

fun p5_tearDownThreadState(execCtx: *ExecutionContext, ts: *P5_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 6 Thread States
// -----------------------------------------------------------------------------

fun p6_initThreadState(execCtx: *ExecutionContext, ts: *P6_ThreadState) -> nil {
    ts.ts_count = 0
    @aggHTInit(&ts.ts_agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
}

fun p6_tearDownThreadState(execCtx: *ExecutionContext, ts: *P6_ThreadState) -> nil {
    @aggHTFree(&ts.ts_agg_table)
}

// -----------------------------------------------------------------------------
// Pipeline 7 Thread States
// -----------------------------------------------------------------------------

fun p7_initThreadState(execCtx: *ExecutionContext, ts: *P7_ThreadState) -> nil {
    ts.ts_count = 0
    @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun p7_tearDownThreadState(execCtx: *ExecutionContext, ts: *P7_ThreadState) -> nil {
    @sorterFree(&ts.ts_sorter)
}

// Scan Region table and build HT1
fun p1_worker(state: *State, ts: *P1_ThreadState, r_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(r_tvi)) {
        var vec = @tableIterGetVPI(r_tvi)

        // Filter
        @filterManagerRunFilters(&ts.filter, vec, execCtx)

        // Insert into HT1
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // r_regionkey
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row1.r_regionkey = @vpiGetInt(vec, 0) // r_regionkey
        }
    }
}

// Scan Nation table, probe HT1, build HT2
fun p2_worker(state: *State, ts: *P2_ThreadState, n_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(n_tvi)) {
        var vec = @tableIterGetVPI(n_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT1
            var hash_val = @hash(@vpiGetInt(vec, 2)) // n_regionkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, state, vec);) {
                var build_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

                // Step 3: Insert into join table 2
                var hash_val2 = @hash(@vpiGetInt(vec, 0)) // n_nationkey
                var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val2))
                build_row2.n_nationkey = @vpiGetInt(vec, 0) // n_nationkey
                build_row2.n_name = @vpiGetString(vec, 1) // n_name
            }
        }
    }
}

// Scan Customer table, probe HT2, build HT3
fun p3_worker(state: *State, ts: *P3_ThreadState, c_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(c_tvi)) {
        var vec = @tableIterGetVPI(c_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT2
            var hash_val = @hash(@vpiGetInt(vec, 3)) // c_nationkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, state, vec);) {
                var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))
                // Step 3: Insert into join table 3
                var hash_val3 = @hash(@vpiGetInt(vec, 0)) // c_custkey
                var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&ts.ts_join_table, hash_val3))
                build_row3.n_nationkey = join_row2.n_nationkey
                build_row3.n_name = join_row2.n_name
                build_row3.c_custkey = @vpiGetInt(vec, 0) // c_custkey
            }
        }
    }
}

// Scan Orders table, probe HT3, build HT4
fun p4_worker(state: *State, ts: *P4_ThreadState, o_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(o_tvi)) {
        var vec = @tableIterGetVPI(o_tvi)

        // Filter
        @filterManagerRunFilters(&ts.filter, vec, execCtx)

        // Probe HT3
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1)) // o_custkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, state, vec);) {
                var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))
                // Step 3: Insert into join table 4
                var hash_val4 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
                var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&ts.ts_join_table, hash_val4))
                build_row4.n_nationkey = join_row3.n_nationkey
                build_row4.n_name = join_row3.n_name
                build_row4.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
                ts.ts_count = ts.ts_count + 1
            }
        }
    }
}

// Scan Supplier, build join HT5
fun p5_worker(state: *State, ts: *P5_ThreadState, s_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(s_tvi)) {
        var vec = @tableIterGetVPI(s_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Insert into HT5
            var hash_val = @hash(@vpiGetInt(vec, 0), @vpiGetInt(vec, 3)) // s_suppkey, s_nationkey
            var build_row5 = @ptrCast(*JoinRow5, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row5.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
            build_row5.s_nationkey = @vpiGetInt(vec, 3) // s_nationkey
            ts.ts_count = ts.ts_count + 1
        }
    }
}


fun p6_worker(state: *State, ts: *P6_ThreadState, l_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(l_tvi)) {
        var vec = @tableIterGetVPI(l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT4
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table4, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey4, state, vec);) {
                var join_row4 = @ptrCast(*JoinRow4, @htEntryIterGetRow(&hti))

                // Step 3: Probe HT5
                var hash_val5 = @hash(@vpiGetInt(vec, 2), join_row4.n_nationkey) // l_suppkey
                var join_probe5 : JoinProbe5 // Materialize the right pipeline
                join_probe5.n_nationkey = join_row4.n_nationkey
                join_probe5.l_suppkey = @vpiGetInt(vec, 2)
                var hti5: HashTableEntryIterator
                for (@joinHTLookup(&state.join_table5, &hti5, hash_val5); @htEntryIterHasNext(&hti5, checkJoinKey5, state, &join_probe5);) {
                    var join_row5 = @ptrCast(*JoinRow5, @htEntryIterGetRow(&hti5))
                    // Step 4: Build Agg HT
                    var agg_input : AggValues // Materialize
                    agg_input.n_name = join_row4.n_name
                    agg_input.revenue = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6)) // l_extendedprice * (1.0 -  l_discount)
                    var agg_hash_val = @hash(join_row4.n_name)
                    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey, &agg_input))
                    if (agg_payload == nil) {
                        agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&ts.ts_agg_table, agg_hash_val))
                        agg_payload.n_name = agg_input.n_name
                        @aggInit(&agg_payload.revenue)
                    }
                    @aggAdvance(&agg_payload.revenue, &agg_input.revenue)
                }
            }
        }
    }
}

fun p6_mergePartitions(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            @aggMerge(&agg_payload.revenue, &partial.revenue)
        }
    }
}

// Scan Agg HT table, sort
fun p7_worker(state: *State, ts: *P7_ThreadState, agg_table: *AggregationHashTable) -> nil {
    var aht_iter: AHTIterator
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&aht_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.ts_sorter))
        sorter_row.n_name = agg_payload.n_name
        sorter_row.revenue = @aggResult(&agg_payload.revenue)
        ts.ts_count = ts.ts_count + 1
    }
    @aggHTIterClose(&aht_iter)
}

// Scan sorter, output
fun pipeline8(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    var out: *OutputStruct
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.n_name = sorter_row.n_name
        out.revenue = sorter_row.revenue

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

    // Pipeline 1
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)
    @iterateTableParallel("region", &state, &tls, p1_worker)
    @joinHTBuildParallel(&state.join_table1, &tls, off)

    // Pipeline 2
    @tlsReset(&tls, @sizeOf(P2_ThreadState), p2_initThreadState, p2_tearDownThreadState, execCtx)
    @iterateTableParallel("nation", &state, &tls, p2_worker)
    @joinHTBuildParallel(&state.join_table2, &tls, off)

    // Pipeline 3
    @tlsReset(&tls, @sizeOf(P3_ThreadState), p3_initThreadState, p3_tearDownThreadState, execCtx)
    @iterateTableParallel("customer", &state, &tls, p3_worker)
    @joinHTBuildParallel(&state.join_table3, &tls, off)

    // Pipeline 4
    @tlsReset(&tls, @sizeOf(P4_ThreadState), p4_initThreadState, p4_tearDownThreadState, execCtx)
    @iterateTableParallel("orders", &state, &tls, p4_worker)
    @joinHTBuildParallel(&state.join_table4, &tls, off)
    //@tlsIterate(&tls, &state, gatherCounters4)

    // Pipeline 5
    @tlsReset(&tls, @sizeOf(P5_ThreadState), p5_initThreadState, p5_tearDownThreadState, execCtx)
    @iterateTableParallel("supplier", &state, &tls, p5_worker)
    @joinHTBuildParallel(&state.join_table5, &tls, off)

    // Pipeline 6
    @tlsReset(&tls, @sizeOf(P6_ThreadState), p6_initThreadState, p6_tearDownThreadState, execCtx)
    @iterateTableParallel("lineitem", &state, &tls, p6_worker)
    @aggHTMoveParts(&state.agg_table, &tls, off, p6_mergePartitions)

    // Pipeline 7
    @tlsReset(&tls, @sizeOf(P7_ThreadState), p7_initThreadState, p7_tearDownThreadState, execCtx)
    @aggHTParallelPartScan(&state.agg_table, &state, &tls, p7_worker)
    @sorterSortParallel(&state.sorter, &tls, off)

    // Pipeline 8
    pipeline8(execCtx, &state)

    // Clean up thread state
    @tlsFree(&tls)

    // Clean up query state
    tearDownState(execCtx, &state)

    return state.count
}
