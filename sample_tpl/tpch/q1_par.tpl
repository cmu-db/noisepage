struct Output {
    l_returnflag   : StringVal
    l_linestatus   : StringVal
    sum_qty        : Real
    sum_base_price : Real
    sum_disc_price : Real
    sum_charge     : Real
    avg_qty        : Real
    avg_price      : Real
    avg_disc       : Real
    count_order    : Integer
}

// Whole-query state
struct State {
    agg_table : AggregationHashTable
    sorter    : Sorter
    count     : int32 // debug
}

// Thread-local state for pipeline 1
struct P1_ThreadState {
    agg_table : AggregationHashTable
    filter    : FilterManager
    count     : int32
}

// Thread-local state for pipeline 2
struct P2_ThreadState {
    sorter: Sorter
}

struct AggValues {
    l_returnflag   : StringVal
    l_linestatus   : StringVal
    sum_qty        : Real
    sum_base_price : Real
    sum_disc_price : Real
    sum_charge     : Real
    avg_qty        : Real
    avg_price      : Real
    avg_disc       : Real
    count_order    : Integer
}

// Structure of row in aggregation hash table
struct AggPayload {
    l_returnflag   : StringVal
    l_linestatus   : StringVal
    sum_qty        : RealSumAggregate
    sum_base_price : RealSumAggregate
    sum_disc_price : RealSumAggregate
    sum_charge     : RealSumAggregate
    avg_qty        : AvgAggregate
    avg_price      : AvgAggregate
    avg_disc       : AvgAggregate
    count_order    : CountAggregate
}

// Structure of rows in sorter
struct SorterRow {
    l_returnflag   : StringVal
    l_linestatus   : StringVal
    sum_qty        : Real
    sum_base_price : Real
    sum_disc_price : Real
    sum_charge     : Real
    avg_qty        : Real
    avg_price      : Real
    avg_disc       : Real
    count_order     : Integer
}

fun compareFn(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.l_returnflag < rhs.l_returnflag) {
        return -1
    }
    if (lhs.l_returnflag > rhs.l_returnflag) {
        return 1
    }
    if (lhs.l_linestatus < rhs.l_linestatus) {
        return -1
    }
    if (lhs.l_linestatus > rhs.l_linestatus) {
        return 1
    }
    return 0
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    var date_filter = @dateToSql(1998, 12, 1)
    @filterLt(execCtx, vector_proj, 10, date_filter, tids)
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTInit(&state.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
}

// -----------------------------------------------------------------------------
// Pipeline 1 Thread States
// -----------------------------------------------------------------------------

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @filterManagerInit(&ts.filter, execCtx)
    @filterManagerInsertFilter(&ts.filter, p1_filter_clause0term0)
    @filterManagerFinalize(&ts.filter)
    @aggHTInit(&ts.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
    ts.count = 0
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @filterManagerFree(&ts.filter)
    @aggHTFree(&ts.agg_table)
}

// -----------------------------------------------------------------------------
// Pipeline 2 Thread States
// -----------------------------------------------------------------------------

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @sorterInit(&ts.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @sorterFree(&ts.sorter)
}

fun p1_constructAggFromPartial(agg_payload: *AggPayload, partial: *AggPayload) -> nil {
    agg_payload.l_linestatus = partial.l_linestatus
    agg_payload.l_returnflag = partial.l_returnflag
    @aggInit(&agg_payload.sum_qty)
    @aggInit(&agg_payload.sum_base_price)
    @aggInit(&agg_payload.sum_disc_price)
    @aggInit(&agg_payload.sum_charge)
    @aggInit(&agg_payload.avg_qty)
    @aggInit(&agg_payload.avg_price)
    @aggInit(&agg_payload.avg_disc)
    @aggInit(&agg_payload.count_order)
}

fun p1_updateAggFromPartial(agg_payload: *AggPayload, partial: *AggPayload) -> nil {
    @aggMerge(&agg_payload.sum_qty, &partial.sum_qty)
    @aggMerge(&agg_payload.sum_base_price, &partial.sum_base_price)
    @aggMerge(&agg_payload.sum_disc_price, &partial.sum_disc_price)
    @aggMerge(&agg_payload.sum_charge, &partial.sum_charge)
    @aggMerge(&agg_payload.avg_qty, &partial.avg_qty)
    @aggMerge(&agg_payload.avg_price, &partial.avg_price)
    @aggMerge(&agg_payload.avg_disc, &partial.avg_disc)
    @aggMerge(&agg_payload.count_order, &partial.count_order)
}

fun aggKeyCheck(agg_payload: *AggPayload, vec: *VectorProjectionIterator) -> bool {
    return agg_payload.l_returnflag == @vpiGetString(vec, 8) and
           agg_payload.l_linestatus == @vpiGetString(vec, 9)
}

fun aggKeyCheckPartial(agg_payload1: *AggPayload, agg_payload2: *AggPayload) -> bool {
    return agg_payload1.l_returnflag == agg_payload2.l_returnflag and
           agg_payload1.l_linestatus == agg_payload2.l_linestatus
}

fun p1_worker(state: *State, ts: *P1_ThreadState, l_tvi: *TableVectorIterator) -> nil {
    var agg_ht = &ts.agg_table
    for (@tableIterAdvance(l_tvi)) {
        var vec = @tableIterGetVPI(l_tvi)

        @filterManagerRunFilters(&ts.filter, vec, execCtx)

        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // l_extendedprice * (1 - l_discount)
            var disc_price = @vpiGetReal(vec, 5) * (@floatToSql(1.0) - @vpiGetReal(vec, 6))

            // l_extendedprice * (1 - l_discount) * (1 + l_tax)
            var charge = disc_price * (@floatToSql(1.0) + @vpiGetReal(vec, 7))

            var agg_values : AggValues
            agg_values.l_returnflag = @vpiGetString(vec, 8) // l_returnflag
            agg_values.l_linestatus = @vpiGetString(vec, 9) // l_linestatus
            agg_values.sum_qty = @vpiGetReal(vec, 4) // l_quantity
            agg_values.sum_base_price = @vpiGetReal(vec, 5) // l_extendedprice
            agg_values.sum_disc_price = disc_price
            agg_values.sum_charge = charge
            agg_values.avg_qty = @vpiGetReal(vec, 4) // l_quantity
            agg_values.avg_price = @vpiGetReal(vec, 5) // l_extendedprice
            agg_values.avg_disc = @vpiGetReal(vec, 6) // l_discount
            agg_values.count_order = @intToSql(1)
            var agg_hash_val = @hash(agg_values.l_returnflag, agg_values.l_linestatus)
            var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_ht, agg_hash_val, aggKeyCheck, vec))
            if (agg_payload == nil) {
                agg_payload = @ptrCast(*AggPayload, @aggHTInsert(agg_ht, agg_hash_val, true))
                agg_payload.l_returnflag = agg_values.l_returnflag
                agg_payload.l_linestatus = agg_values.l_linestatus
                @aggInit(&agg_payload.sum_qty)
                @aggInit(&agg_payload.sum_base_price)
                @aggInit(&agg_payload.sum_disc_price)
                @aggInit(&agg_payload.sum_charge)
                @aggInit(&agg_payload.avg_qty)
                @aggInit(&agg_payload.avg_price)
                @aggInit(&agg_payload.avg_disc)
                @aggInit(&agg_payload.count_order)
            }
            @aggAdvance(&agg_payload.sum_qty, &agg_values.sum_qty)
            @aggAdvance(&agg_payload.sum_base_price, &agg_values.sum_base_price)
            @aggAdvance(&agg_payload.sum_disc_price, &agg_values.sum_disc_price)
            @aggAdvance(&agg_payload.sum_charge, &agg_values.sum_charge)
            @aggAdvance(&agg_payload.avg_qty, &agg_values.avg_qty)
            @aggAdvance(&agg_payload.avg_price, &agg_values.avg_price)
            @aggAdvance(&agg_payload.avg_disc, &agg_values.avg_disc)
            @aggAdvance(&agg_payload.count_order, &agg_values.count_order)
        }
    }
}

fun p1_mergePartitions(state: *State, agg_table: *AggregationHashTable, iter: *AHTOverflowPartitionIterator) -> nil {
    var x = 0
    for (; @aggPartIterHasNext(iter); @aggPartIterNext(iter)) {
        var partial_hash = @aggPartIterGetHash(iter)
        var partial = @ptrCast(*AggPayload, @aggPartIterGetRow(iter))
        var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(agg_table, partial_hash, aggKeyCheckPartial, partial))
        if (agg_payload == nil) {
            @aggHTLink(agg_table, @aggPartIterGetRowEntry(iter))
        } else {
            p1_updateAggFromPartial(agg_payload, partial)
        }
    }
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // Initialize thread states for pipeline 1
    var tls: ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)

    // Parallel scan
    @iterateTableParallel("lineitem", state, &tls, p1_worker)

    // Build global aggregation hash table
    var aht_off: uint32 = 0
    @aggHTMoveParts(&state.agg_table, &tls, aht_off, p1_mergePartitions)

    // Cleanup thread-local states
    @tlsFree(&tls)
}

fun p2_worker(state: *State, ts: *P2_ThreadState, agg_table: *AggregationHashTable) -> nil {
    // Pipeline 2 (Sorting)
    var sorter = &ts.sorter
    var agg_iter: AHTIterator
    for (@aggHTIterInit(&agg_iter, agg_table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(&agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(sorter))
        sorter_row.l_returnflag = agg_payload.l_returnflag
        sorter_row.l_linestatus = agg_payload.l_linestatus
        sorter_row.sum_qty = @aggResult(&agg_payload.sum_qty)
        sorter_row.sum_base_price = @aggResult(&agg_payload.sum_base_price)
        sorter_row.sum_disc_price = @aggResult(&agg_payload.sum_disc_price)
        sorter_row.sum_charge = @aggResult(&agg_payload.sum_charge)
        sorter_row.avg_qty = @aggResult(&agg_payload.avg_qty)
        sorter_row.avg_price = @aggResult(&agg_payload.avg_price)
        sorter_row.avg_disc = @aggResult(&agg_payload.avg_disc)
        sorter_row.count_order = @aggResult(&agg_payload.count_order)
    }
    @aggHTIterClose(&agg_iter)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Initialize thread states for pipeline 2
    var tls: ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P2_ThreadState), p2_initThreadState, p2_tearDownThreadState, execCtx)

    // Scan aggregation hash table and insert into thread-local sorters
    @aggHTParallelPartScan(&state.agg_table, state, &tls, p2_worker)

    // Parallel sort
    var sorter_off : uint32 = 0
    @sorterSortParallel(&state.sorter, &tls, sorter_off)

    // Cleanup thread-local states
    @tlsFree(&tls)
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 3 (Output to upper layers)
    var out: *Output
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        state.count = state.count + 1

        out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.l_returnflag = sorter_row.l_returnflag
        out.l_linestatus = sorter_row.l_linestatus
        out.sum_qty = sorter_row.sum_qty
        out.sum_base_price = sorter_row.sum_base_price
        out.sum_disc_price = sorter_row.sum_disc_price
        out.sum_charge = sorter_row.sum_charge
        out.avg_qty = sorter_row.avg_qty
        out.avg_price = sorter_row.avg_price
        out.avg_disc = sorter_row.avg_disc
        out.count_order = sorter_row.count_order
    }
    @sorterIterClose(&sort_iter)

    @resultBufferFinalize(execCtx)
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
    pipeline3(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
