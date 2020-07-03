struct OutputStruct {
    c_name : StringVal
    c_custkey : Integer
    o_orderkey : Integer
    o_orderdate : Date
    o_totalprice : Real
    sum_quantity : Real
}

struct State {
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    join_table3 : JoinHashTable
    agg_table1  : AggregationHashTable
    agg_table2  : AggregationHashTable
    sorter      : Sorter
    count       : int32 // Debug
}

struct P1_ThreadState {
    ts_agg_table : AggregationHashTable
}

struct P2_ThreadState {
    ts_join_table : JoinHashTable
}

struct P3_ThreadState {
    ts_join_table : JoinHashTable
}

struct P4_ThreadState {
    ts_join_table : JoinHashTable
}

struct P5_ThreadState {
    ts_agg_table : AggregationHashTable
}

struct P6_ThreadState {
    ts_sorter : Sorter
}

struct JoinRow1 {
    l_orderkey : Integer
}

struct JoinRow2 {
    c_custkey : Integer
    c_name    : StringVal
}

struct JoinRow3 {
    c_custkey    : Integer
    c_name       : StringVal
    o_orderkey   : Integer
    o_orderdate  : Date
    o_totalprice : Real
}

struct AggValues1 {
    l_orderkey   : Integer
    sum_quantity : Real
}

struct AggPayload1 {
    l_orderkey   : Integer
    sum_quantity : RealSumAggregate
}

struct AggValues2 {
    c_name       : StringVal
    c_custkey    : Integer
    o_orderkey   : Integer
    o_orderdate  : Date
    o_totalprice : Real
    sum_quantity : Real
}

struct AggPayload2 {
    c_name       : StringVal
    c_custkey    : Integer
    o_orderkey   : Integer
    o_orderdate  : Date
    o_totalprice : Real
    sum_quantity : RealSumAggregate
}

struct SorterRow {
    c_name       : StringVal
    c_custkey    : Integer
    o_orderkey   : Integer
    o_orderdate  : Date
    o_totalprice : Real
    sum_quantity : Real
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // o_orderkey == l_orderkey
    if (@vpiGetInt(probe, 0) != build.l_orderkey) {
        return false
    }
    return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
    // o_custkey != c_custkey
    if (@vpiGetInt(probe, 1) != build.c_custkey) {
        return false
    }
    return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow3) -> bool {
    // l_orderkey == o_orderkey
    if (@vpiGetInt(probe, 0) != build.o_orderkey) {
        return false
    }
    return true
}

fun checkAggKey1(payload: *AggPayload1, row: *AggValues1) -> bool {
    if (payload.l_orderkey != row.l_orderkey) {
        return false
    }
    return true
}

fun aggKeyCheckPartial1(agg_payload1: *AggPayload1, agg_payload2: *AggPayload1) -> bool {
    return agg_payload1.l_orderkey == agg_payload2.l_orderkey
}

fun checkAggKey2(payload: *AggPayload2, row: *JoinRow3) -> bool {
    if (payload.c_custkey != row.c_custkey) {
        return false
    }
    if (payload.o_orderkey != row.o_orderkey) {
        return false
    }
    if (payload.o_orderdate != row.o_orderdate) {
        return false
    }
    if (payload.o_totalprice != row.o_totalprice) {
        return false
    }
    if (payload.c_name != row.c_name) {
        return false
    }
    return true
}

fun aggKeyCheckPartial2(agg_payload1: *AggPayload2, agg_payload2: *AggPayload2) -> bool {
    return (agg_payload1.c_custkey == agg_payload2.c_custkey) and
           (agg_payload1.o_orderkey == agg_payload2.o_orderkey) and
           (agg_payload1.o_orderdate == agg_payload2.o_orderdate) and
           (agg_payload1.o_totalprice == agg_payload2.o_totalprice) and
           (agg_payload1.c_name == agg_payload2.c_name)
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.o_totalprice < rhs.o_totalprice) {
        return 1 // desc
    }
    if (lhs.o_totalprice > rhs.o_totalprice) {
        return -1 // desc
    }
    if (lhs.o_orderdate < rhs.o_orderdate) {
        return -1
    }
    if (lhs.o_orderdate > rhs.o_orderdate) {
        return 1
    }
    return 0
}

// -----------------------------------------------------------------------------
// Query State
// -----------------------------------------------------------------------------

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
    @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
    @aggHTInit(&state.agg_table1, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload1))
    @aggHTInit(&state.agg_table2, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @joinHTFree(&state.join_table3)
    @aggHTFree(&state.agg_table1)
    @aggHTFree(&state.agg_table2)
    @sorterFree(&state.sorter)
}

// -----------------------------------------------------------------------------
// Pipeline 1 State
// -----------------------------------------------------------------------------

fun pt_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @aggHTInit(&ts.ts_agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload1))
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @aggHTFree(&ts.ts_agg_table)
}

// -----------------------------------------------------------------------------
// Pipeline 2 State
// -----------------------------------------------------------------------------

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 3 State
// -----------------------------------------------------------------------------

fun p3_initThreadState(execCtx: *ExecutionContext, ts: *P3_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
}

fun p3_tearDownThreadState(execCtx: *ExecutionContext, ts: *P3_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 4 State
// -----------------------------------------------------------------------------

fun p4_initThreadState(execCtx: *ExecutionContext, ts: *P4_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
}

fun p4_tearDownThreadState(execCtx: *ExecutionContext, ts: *P3_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 5 State
// -----------------------------------------------------------------------------

fun p5_initThreadState(execCtx: *ExecutionContext, ts: *P5_ThreadState) -> nil {
    @aggHTInit(&ts.ts_agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload2))
}

fun p5_tearDownThreadState(execCtx: *ExecutionContext, ts: *P5_ThreadState) -> nil {
    @aggHTFree(&ts.ts_agg_table)
}

// -----------------------------------------------------------------------------
// Pipeline 6 State
// -----------------------------------------------------------------------------

fun p6_initThreadState(execCtx: *ExecutionContext, ts: *P6_ThreadState) -> nil {
    @sorterInit(&ts.ts_sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun p6_tearDownThreadState(execCtx: *ExecutionContext, ts: *P6_ThreadState) -> nil {
    @sorterFree(&ts.ts_sorter)
}

// -----------------------------------------------------------------------------
// Pipeline 1
// -----------------------------------------------------------------------------

// Scan lineitem, build AHT1
fun p1_worker(state: *State, ts: *P1_ThreadState, l_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(l_tvi)) {
        var vec = @tableIterGetVPI(l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var agg_input : AggValues1
            agg_input.l_orderkey = @vpiGetInt(vec, 0) // l_orderkey
            agg_input.sum_quantity = @vpiGetReal(vec, 4) // l_quantity
            var agg_hash_val = @hash(agg_input.l_orderkey)
            var agg_payload = @ptrCast(*AggPayload1, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey1, &agg_input))
            if (agg_payload == nil) {
                agg_payload = @ptrCast(*AggPayload1, @aggHTInsert(&ts.ts_agg_table, agg_hash_val, true))
                agg_payload.l_orderkey = agg_input.l_orderkey
                @aggInit(&agg_payload.sum_quantity)
            }
            @aggAdvance(&agg_payload.sum_quantity, &agg_input.sum_quantity)
        }
    }
}

fun mergerPartitions1(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggPayload1, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggPayload1, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial1, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            @aggMerge(&agg_payload.sum_quantity, &partial.sum_quantity)
        }
    }
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), pt_initThreadState, p1_tearDownThreadState, execCtx)

    // Parallel scan
    @iterateTableParallel("lineitem", state, &tls, p1_worker)

    // Merge AHT partitions
    var off: uint32 = 0
    @aggHTMoveParts(&state.agg_table1, &tls, off, mergerPartitions1)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 2
// -----------------------------------------------------------------------------

// Scan AHT1, Build JHT1
fun p2_worker(state: *State, ts: *P2_ThreadState, agg_table: *AggregationHashTable) -> nil {
    var aht_iter: AHTIterator
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg_payload = @ptrCast(*AggPayload1, @aggHTIterGetRow(&aht_iter))
        if (@aggResult(&agg_payload.sum_quantity) > 300.0) {
            var hash_val = @hash(agg_payload.l_orderkey)
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row1.l_orderkey = agg_payload.l_orderkey
        }
    }
    @aggHTIterClose(&aht_iter)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P2_ThreadState), p2_initThreadState, p2_tearDownThreadState, execCtx)

    // Parallel scan aggregation table
    @aggHTParallelPartScan(&state.agg_table1, state, &tls, p2_worker)

    // Parallel build join table
    var off: uint32 = 0
    @joinHTBuildParallel(&state.join_table1, &tls, off)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 3
// -----------------------------------------------------------------------------

// Scan customer, build JHT2
fun p3_worker(state: *State, ts: *P3_ThreadState, c_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(c_tvi)) {
        var vec = @tableIterGetVPI(c_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // c_custkey
            var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row2.c_custkey = @vpiGetInt(vec, 0) // c_custkey
            build_row2.c_name = @vpiGetString(vec, 1) // c_name
        }
    }
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P3_ThreadState), p3_initThreadState, p3_tearDownThreadState, execCtx)

    // Parallel scan 'customer'
    @iterateTableParallel("customer", state, &tls, p3_worker)

    // Parallel build join table
    var off: uint32 = 0
    @joinHTBuildParallel(&state.join_table2, &tls, off)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 4
// -----------------------------------------------------------------------------

// Scan orders, probe JHT1, probe JHT2, build JHT3
fun p4_worker(state: *State, ts: *P4_ThreadState, o_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(o_tvi)) {
        var vec = @tableIterGetVPI(o_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // o_orderkey
            var hti: HashTableEntryIterator
            // Semi-Join with JHT1
            @joinHTLookup(&state.join_table1, &hti, hash_val)
            if (@htEntryIterHasNext(&hti, checkJoinKey1, state, vec)) {
                var hash_val2 = @hash(@vpiGetInt(vec, 1)) // o_custkey
                var hti2: HashTableEntryIterator
                for (@joinHTLookup(&state.join_table2, &hti2, hash_val2); @htEntryIterHasNext(&hti2, checkJoinKey2, state, vec);) {
                    var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti2))

                    // Build JHT3
                    var hash_val3 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
                    var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&ts.ts_join_table, hash_val3))
                    build_row3.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
                    build_row3.o_orderdate = @vpiGetDate(vec, 4) // o_orderdate
                    build_row3.o_totalprice = @vpiGetReal(vec, 3) // o_totalprice
                    build_row3.c_custkey = join_row2.c_custkey
                    build_row3.c_name = join_row2.c_name
                }
            }
        }
    }
}

fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P4_ThreadState), p4_initThreadState, p4_tearDownThreadState, execCtx)

    // Parallel scan 'orders'
    @iterateTableParallel("orders", state, &tls, p4_worker)

    // Parallel build join table
    var off: uint32 = 0
    @joinHTBuildParallel(&state.join_table3, &tls, off)

    //@tlsIterate(&tls, &state, gatherCounters4)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 5
// -----------------------------------------------------------------------------

fun mergerPartitions5(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggPayload2, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial2, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            @aggMerge(&agg_payload.sum_quantity, &partial.sum_quantity)
        }
    }
}

// Scan lineitem, probe JHT3, build AHT2
fun p5_worker(state: *State, ts: *P5_ThreadState, l_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(l_tvi)) {
        var vec = @tableIterGetVPI(l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, state, vec);) {
                var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))

                // Build AHT
                var agg_hash_val = @hash(join_row3.c_name, join_row3.c_custkey, join_row3.o_orderkey, join_row3.o_orderdate, join_row3.o_totalprice)
                var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(&ts.ts_agg_table, agg_hash_val, checkAggKey2, join_row3))
                if (agg_payload == nil) {
                    agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(&ts.ts_agg_table, agg_hash_val, true))
                    agg_payload.c_name = join_row3.c_name
                    agg_payload.c_custkey = join_row3.c_custkey
                    agg_payload.o_orderkey = join_row3.o_orderkey
                    agg_payload.o_orderdate = join_row3.o_orderdate
                    agg_payload.o_totalprice = join_row3.o_totalprice
                    @aggInit(&agg_payload.sum_quantity)
                }
                var l_quantity = @vpiGetReal(vec, 4)
                @aggAdvance(&agg_payload.sum_quantity, &l_quantity)
            }
        }
    }
}

fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P5_ThreadState), p5_initThreadState, p5_tearDownThreadState, execCtx)

    // Parallel scan 'lineitem'
    @iterateTableParallel("lineitem", state, &tls, p5_worker)

    // Parallel merge AHT
    var off: uint32 = 0
    @aggHTMoveParts(&state.agg_table2, &tls, off, mergerPartitions5)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 6
// -----------------------------------------------------------------------------

// Scan AHT2, sort
fun p6_worker(state: *State, ts: *P6_ThreadState, agg_table: *AggregationHashTable) -> nil {
    var aht_iter: AHTIterator
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg_payload = @ptrCast(*AggPayload2, @aggHTIterGetRow(&aht_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsertTopK(&ts.ts_sorter, 100))
        sorter_row.c_name = agg_payload.c_name
        sorter_row.c_custkey = agg_payload.c_custkey
        sorter_row.o_orderkey = agg_payload.o_orderkey
        sorter_row.o_orderdate = agg_payload.o_orderdate
        sorter_row.o_totalprice = agg_payload.o_totalprice
        sorter_row.sum_quantity = @aggResult(&agg_payload.sum_quantity)
        @sorterInsertTopKFinish(&ts.ts_sorter, 100)
        //ts.ts_count = ts.ts_count + 1
    }
    @aggHTIterClose(&aht_iter)
}

fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
    // Thread-local state
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P6_ThreadState), p6_initThreadState, p6_tearDownThreadState, execCtx)

    // Parallel scan AHT
    @aggHTParallelPartScan(&state.agg_table2, state, &tls, p6_worker)

    // Parallel sort
    var off: uint32 = 0
    @sorterSortTopKParallel(&state.sorter, &tls, off, 100)

    // Cleanup
    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 7
// -----------------------------------------------------------------------------

// Iterate through sorter, output
fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        var out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        out.c_name = sorter_row.c_name
        out.c_custkey = sorter_row.c_custkey
        out.o_orderkey = sorter_row.o_orderkey
        out.o_orderdate = sorter_row.o_orderdate
        out.o_totalprice = sorter_row.o_totalprice
        out.sum_quantity = sorter_row.sum_quantity
        state.count = state.count + 1
    }
    @resultBufferFinalize(execCtx)
}

// -----------------------------------------------------------------------------
// Main and Launch
// -----------------------------------------------------------------------------

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
    pipeline3(execCtx, state)
    pipeline4(execCtx, state)
    pipeline5(execCtx, state)
    pipeline6(execCtx, state)
    pipeline7(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
