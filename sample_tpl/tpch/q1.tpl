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

struct State {
    filter         : FilterManager
    agg_hash_table : AggregationHashTable
    sorter         : Sorter
    count          : int32 // debug
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
    count_order    : Integer
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

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj : *VectorProjection, tids: *TupleIdList, ctx: *uint8) -> nil {
    var date_filter = @dateToSql(1998, 9, 2)
    @filterLt(execCtx, vector_proj, 10, date_filter, tids)
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @filterManagerInit(&state.filter, execCtx)
    @filterManagerInsertFilter(&state.filter, p1_filter_clause0term0)
    @filterManagerFinalize(&state.filter)
    @aggHTInit(&state.agg_hash_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @filterManagerFree(&state.filter)
    @aggHTFree(&state.agg_hash_table)
    @sorterFree(&state.sorter)
}

fun aggKeyCheck(agg_payload: *AggPayload, agg_values: *AggValues) -> bool {
    if (agg_payload.l_returnflag != agg_values.l_returnflag) {
        return false
    }
    if (agg_payload.l_linestatus != agg_values.l_linestatus) {
        return false
    }
    return true
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 1 (Aggregating)
    var l_tvi : TableVectorIterator
    @tableIterInit(&l_tvi, "lineitem")
    for (@tableIterAdvance(&l_tvi)) {
        var vec = @tableIterGetVPI(&l_tvi)

        // Filter on 'l_shipdate'
        @filterManagerRunFilters(&state.filter, vec, execCtx)

        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
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
            var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_hash_table, agg_hash_val, aggKeyCheck, &agg_values))
            if (agg_payload == nil) {
                agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_hash_table, agg_hash_val))
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
    @tableIterClose(&l_tvi)
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 2 (Sorting)
    var agg_iter: AHTIterator
    for (@aggHTIterInit(&agg_iter, &state.agg_hash_table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(&agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
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

    // Sort!
    @sorterSort(&state.sorter)
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

fun main(execCtx: *ExecutionContext) -> int {
    var state: State

    setUpState(execCtx, &state)
    pipeline1(execCtx, &state)
    pipeline2(execCtx, &state)
    pipeline3(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
