struct OutputStruct {
    o_orderpriority : StringVal
    order_count     : Integer
}

struct JoinBuildRow {
    o_orderkey      : Integer
    o_orderpriority : StringVal
    match_flag      : bool
}

// Input & Output of the aggregator
struct AggRow {
    o_orderpriority : StringVal
    order_count     : CountStarAggregate
}

// Input & output of the sorter
struct SorterRow {
    o_orderpriority : StringVal
    order_count     : Integer
}

struct State {
    join_table : JoinHashTable
    agg_table  : AggregationHashTable
    sorter     : Sorter
    count      : int32  // debug
}

struct ThreadState1 {
    join_table : JoinHashTable
    filter     : FilterManager
}

struct ThreadState2 {
    agg_table  : AggregationHashTable
    filter     : FilterManager
}

struct ThreadState3 {
    sorter : Sorter
}

// Check that two join keys are equal
fun checkJoinKey(state: *State, probe: *VectorProjectionIterator, build_row: *JoinBuildRow) -> bool {
    // l_orderkey == o_orderkey
    return @sqlToBool(@vpiGetInt(probe, 0) == build_row.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggRow, build_row: *JoinBuildRow) -> bool {
    return @sqlToBool(agg.o_orderpriority == build_row.o_orderpriority)
}

fun aggKeyCheckPartial(agg_payload1: *AggRow, agg_payload2: *AggRow) -> bool {
    return @sqlToBool(agg_payload1.o_orderpriority == agg_payload2.o_orderpriority)
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.o_orderpriority < rhs.o_orderpriority) {
        return -1
    } else if (lhs.o_orderpriority > lhs.o_orderpriority) {
        return 1
    }
    return 0
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    var orderdate_lo = @dateToSql(1993, 7, 1)
    @filterGe(execCtx, vector_proj, 4, orderdate_lo, tids)
}

fun p1_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    var orderdate_hi = @dateToSql(1993, 10, 1)
    @filterLe(execCtx, vector_proj, 4, orderdate_hi, tids)
}

fun p2_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_commitdate < l_receiptdate
    @filterLt(execCtx, vector_proj, 11, 12, tids)
}

// Setup the whole-query state
fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggRow))
    @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
    state.count = 0
}

// Tear down the whole-query state
fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
    @joinHTFree(&state.join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 1 Thread States
// -----------------------------------------------------------------------------

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p1_filter_clause0term0, p1_filter_clause0term1)
    @filterManagerFinalize(&ts.filter)
    @joinHTInit(&ts.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *ThreadState1) -> nil {
    @filterManagerFree(&ts.filter)
    @joinHTFree(&ts.join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 2 Thread States
// -----------------------------------------------------------------------------

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p2_filter_clause0term0)
    @filterManagerFinalize(&ts.filter)
    @aggHTInit(&ts.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggRow))
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *ThreadState2) -> nil {
    @filterManagerFree(&ts.filter)
    @aggHTFree(&ts.agg_table)
}

// -----------------------------------------------------------------------------
// Pipeline 3 Thread States
// -----------------------------------------------------------------------------

fun p3_initThreadState(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
    @sorterInit(&ts.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun p3_tearDownThreadState(execCtx: *ExecutionContext, ts: *ThreadState3) -> nil {
    @sorterFree(&ts.sorter)
}

// Pipeline 1 Worker (Join Build)
fun p1_worker(state: *State, ts: *ThreadState1, o_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(o_tvi)) {
        var vec = @tableIterGetVPI(o_tvi)

        // Run Filter
        @filterManagerRunFilters(&ts.filter, vec, execCtx)

        // Insert into JHT
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // o_orderkey
            var build_row = @ptrCast(*JoinBuildRow, @joinHTInsert(&ts.join_table, hash_val))
            build_row.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
            build_row.o_orderpriority = @vpiGetString(vec, 5) // o_orderpriority
            build_row.match_flag = false
        }
    }
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState1), p1_initThreadState, p1_tearDownThreadState, execCtx)

    // Parallel scan + join build
    @iterateTableParallel("orders", state, &tls, p1_worker)

    // Parallel Build
    var off: uint32 = 0
    @joinHTBuildParallel(&state.join_table, &tls, off)

    // Cleanup thread-local states
    @tlsFree(&tls)
}

// Pipeline 2 (Join Probe up to Agg)
fun p2_worker(state: *State, ts: *ThreadState2, l_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(l_tvi)) {
        var vec = @tableIterGetVPI(l_tvi)

        // Run Filter
        @filterManagerRunFilters(&ts.filter, vec, execCtx)

        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var join_iter: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table, &join_iter, hash_val);
                 @htEntryIterHasNext(&join_iter, checkJoinKey, state, vec);) {

                var build_row = @ptrCast(*JoinBuildRow, @htEntryIterGetRow(&join_iter))
                // match each row once
                if (!build_row.match_flag) {
                    build_row.match_flag = true
                    // Step 3: Build Agg Hash Table
                    var agg_hash_val = @hash(build_row.o_orderpriority)
                    var agg = @ptrCast(*AggRow, @aggHTLookup(&ts.agg_table, agg_hash_val, checkAggKey, build_row))
                    if (agg == nil) {
                        agg = @ptrCast(*AggRow, @aggHTInsert(&ts.agg_table, agg_hash_val))
                        agg.o_orderpriority = build_row.o_orderpriority
                        @aggInit(&agg.order_count)
                    }
                    @aggAdvance(&agg.order_count, &build_row.o_orderpriority)
                }
            }
        }
    }
}

fun p2_mergePartitions(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggRow, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggRow, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            @aggMerge(&agg_payload.order_count, &partial.order_count)
        }
    }
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Initialize thread-local states
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState2), p2_initThreadState, p2_tearDownThreadState, execCtx)

    // Parallel scan + join probe + agg build
    @iterateTableParallel("lineitem", state, &tls, p2_worker)

    // Move thread-local tables into global table
    var aht_off: uint32 = 0
    @aggHTMoveParts(&state.agg_table, &tls, aht_off, p2_mergePartitions)

    // Cleanup thread-local states
    @tlsFree(&tls)
}

// Pipeline 3 (Sort)
fun p3_worker(state: *State, ts: *ThreadState3, agg_table: *AggregationHashTable) -> nil {
    // Step 1: Iterate through Agg Hash Table
    var aht_iter: AHTIterator
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg = @ptrCast(*AggRow, @aggHTIterGetRow(&aht_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&ts.sorter))
        sorter_row.o_orderpriority = agg.o_orderpriority
        sorter_row.order_count = @aggResult(&agg.order_count)
    }
    @aggHTIterClose(&aht_iter)
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Initialize thread-local states
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(ThreadState3), p3_initThreadState, p3_tearDownThreadState, execCtx)

    // Scan AHT and fill up sorters
    @aggHTParallelPartScan(&state.agg_table, state, &tls, p3_worker)

    // Parallel sort
    var sorter_off : uint32 = 0
    @sorterSortParallel(&state.sorter, &tls, sorter_off)

    // Cleanup thread-local states
    @tlsFree(&tls)
}

// Pipeline 4 (Output)
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter);
         @sorterIterHasNext(&sort_iter);
         @sorterIterNext(&sort_iter)) {

        var out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.o_orderpriority = sorter_row.o_orderpriority
        out.order_count = sorter_row.order_count
        state.count = state.count + 1
    }
    @sorterIterClose(&sort_iter)

    // Finish
    @resultBufferFinalize(execCtx)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
    pipeline3(execCtx, state)
    pipeline4(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
