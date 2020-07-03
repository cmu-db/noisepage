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
    count      : int32
}

// Check that two join keys are equal
fun checkJoinKey(execCtx: *ExecutionContext, probe: *VectorProjectionIterator,
                 build_row: *JoinBuildRow) -> bool {
    // l_orderkey == o_orderkey
    return @sqlToBool(@vpiGetInt(probe, 0) == build_row.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggRow, build_row: *JoinBuildRow) -> bool {
    return @sqlToBool(agg.o_orderpriority == build_row.o_orderpriority)
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

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggRow))
    @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
    state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
    @joinHTFree(&state.join_table)
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    var orderdate_lower = @dateToSql(1993, 7, 1)
    @filterGe(execCtx, vector_proj, 4, orderdate_lower, tids)
}

fun p1_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    var orderdate_upper = @dateToSql(1993, 10, 1)
    @filterLt(execCtx, vector_proj, 4, orderdate_upper, tids)
}

// Pipeline 1 (Join Build)
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, p1_filter_clause0term0, p1_filter_clause0term1)
    @filterManagerFinalize(&filter)

    var o_tvi : TableVectorIterator
    @tableIterInit(&o_tvi, "orders")
    for (@tableIterAdvance(&o_tvi)) {
        var vec = @tableIterGetVPI(&o_tvi)

        // Filter
        @filterManagerRunFilters(&filter, vec, execCtx)

        // Build JHT
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var orderkey = @vpiGetInt(vec, 0)
            var hash_val = @hash(orderkey) // o_orderkey
            var build_row = @ptrCast(*JoinBuildRow, @joinHTInsert(&state.join_table, hash_val))
            build_row.o_orderkey = orderkey
            build_row.o_orderpriority = @vpiGetString(vec, 5) // o_orderpriority
            build_row.match_flag = false
        }
    }
    @tableIterClose(&o_tvi)

    // Build hash table
    @joinHTBuild(&state.join_table)

    // Cleanup
    @filterManagerFree(&filter)
}

fun p2_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_commitdate < l_receiptdate
    @filterLt(execCtx, vector_proj, 11, 12, tids)
}

// Pipeline 2 (Join Probe up to Agg)
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, p2_filter_clause0term0)
    @filterManagerFinalize(&filter)

    var l_tvi : TableVectorIterator
    for (@tableIterInit(&l_tvi, "lineitem"); @tableIterAdvance(&l_tvi); ) {
        var vec = @tableIterGetVPI(&l_tvi)

        // Filter
        @filterManagerRunFilters(&filter, vec, execCtx)

        // Probe + Aggregate
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var join_iter: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table, &join_iter, hash_val);
                 @htEntryIterHasNext(&join_iter, checkJoinKey, execCtx, vec); ) {
                var build_row = @ptrCast(*JoinBuildRow, @htEntryIterGetRow(&join_iter))
                // match each row once
                if (!build_row.match_flag) {
                    build_row.match_flag = true
                    // Step 3: Build Agg Hash Table
                    var agg_hash_val = @hash(build_row.o_orderpriority)
                    var agg = @ptrCast(*AggRow, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, build_row))
                    if (agg == nil) {
                        agg = @ptrCast(*AggRow, @aggHTInsert(&state.agg_table, agg_hash_val))
                        agg.o_orderpriority = build_row.o_orderpriority
                        @aggInit(&agg.order_count)
                    }
                    @aggAdvance(&agg.order_count, &build_row.o_orderpriority)
                }
            }
        }
    }
    @tableIterClose(&l_tvi)

    // Cleanup
    @filterManagerFree(&filter)
}

// Pipeline 3 (Sort)
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Step 1: Iterate through Agg Hash Table
    var agg_table = &state.agg_table
    var aht_iter: AHTIterator
    for (@aggHTIterInit(&aht_iter, agg_table); @aggHTIterHasNext(&aht_iter); @aggHTIterNext(&aht_iter)) {
        var agg = @ptrCast(*AggRow, @aggHTIterGetRow(&aht_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
        sorter_row.o_orderpriority = agg.o_orderpriority
        sorter_row.order_count = @aggResult(&agg.order_count)
    }
    @aggHTIterClose(&aht_iter)

    // Sort
    @sorterSort(&state.sorter)
}

// Pipeline 4 (Output)
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    var out: *OutputStruct
    for (@sorterIterInit(&sort_iter, &state.sorter);
         @sorterIterHasNext(&sort_iter);
         @sorterIterNext(&sort_iter)) {
        state.count = state.count + 1

        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))

        // Output
        out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        out.o_orderpriority = sorter_row.o_orderpriority
        out.order_count = sorter_row.order_count
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
    teardownState(execCtx, &state)

    return state.count
}
