struct OutputStruct {
    c_name       : StringVal
    c_custkey    : Integer
    o_orderkey   : Integer
    o_orderdate  : Date
    o_totalprice : Real
    sum_quantity : Real
}

struct State {
    count       : int32 // Debug
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    join_table3 : JoinHashTable
    agg_table1  : AggregationHashTable
    agg_table2  : AggregationHashTable
    sorter      : Sorter
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

fun checkAggKey2(payload: *AggPayload2, row: *AggValues2) -> bool {
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

// Scan lineitem, build AHT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var l_tvi : TableVectorIterator
    for (@tableIterInit(&l_tvi, "lineitem"); @tableIterAdvance(&l_tvi); ) {
        var vec = @tableIterGetVPI(&l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var agg_input : AggValues1
            agg_input.l_orderkey = @vpiGetInt(vec, 0) // l_orderkey
            agg_input.sum_quantity = @vpiGetReal(vec, 4) // l_quantity
            var agg_hash_val = @hash(agg_input.l_orderkey)
            var agg_payload = @ptrCast(*AggPayload1, @aggHTLookup(&state.agg_table1, agg_hash_val, checkAggKey1, &agg_input))
            if (agg_payload == nil) {
                agg_payload = @ptrCast(*AggPayload1, @aggHTInsert(&state.agg_table1, agg_hash_val))
                agg_payload.l_orderkey = agg_input.l_orderkey
                @aggInit(&agg_payload.sum_quantity)
            }
            @aggAdvance(&agg_payload.sum_quantity, &agg_input.sum_quantity)
        }
    }
    @tableIterClose(&l_tvi)
}

// Scan AHT1, Build JHT1
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var agg_ht_iter: AHTIterator
    var agg_iter = &agg_ht_iter
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(agg_iter, &state.agg_table1); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload1, @aggHTIterGetRow(agg_iter))
        if (@aggResult(&agg_payload.sum_quantity) > 300.0) {
            var hash_val = @hash(agg_payload.l_orderkey)
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
            build_row1.l_orderkey = agg_payload.l_orderkey
        }
    }
    @joinHTBuild(&state.join_table1)
    @aggHTIterClose(agg_iter)
}

// Scan customer, build JHT2
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    var c_tvi : TableVectorIterator
    for (@tableIterInit(&c_tvi, "customer"); @tableIterAdvance(&c_tvi); ) {
        var vec = @tableIterGetVPI(&c_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // c_custkey
            var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val))
            build_row2.c_custkey = @vpiGetInt(vec, 0) // c_custkey
            build_row2.c_name = @vpiGetString(vec, 1) // c_name
        }
    }
    @joinHTBuild(&state.join_table2)
    @tableIterClose(&c_tvi)
}

// Scan orders, probe JHT1, probe JHT2, build JHT3
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var o_tvi : TableVectorIterator
    for (@tableIterInit(&o_tvi, "orders"); @tableIterAdvance(&o_tvi); ) {
        var vec = @tableIterGetVPI(&o_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // o_orderkey
            var hti: HashTableEntryIterator
            // Semi-Join with JHT1
            @joinHTLookup(&state.join_table1, &hti, hash_val)
            if (@htEntryIterHasNext(&hti, checkJoinKey1, execCtx, vec)) {
                var hash_val2 = @hash(@vpiGetInt(vec, 1)) // o_custkey
                var hti2: HashTableEntryIterator
                for (@joinHTLookup(&state.join_table2, &hti2, hash_val2); @htEntryIterHasNext(&hti2, checkJoinKey2, execCtx, vec);) {
                    var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti2))

                    // Build JHT3
                    var hash_val3 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
                    var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table3, hash_val3))
                    build_row3.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
                    build_row3.o_orderdate = @vpiGetDate(vec, 4) // o_orderdate
                    build_row3.o_totalprice = @vpiGetReal(vec, 3) // o_totalprice
                    build_row3.c_custkey = join_row2.c_custkey
                    build_row3.c_name = join_row2.c_name
                }
            }
        }
    }
    @tableIterClose(&o_tvi)

    // Build JHT
    @joinHTBuild(&state.join_table3)

}

// Scan lineitem, probe JHT3, build AHT2
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    var l_tvi : TableVectorIterator
    for (@tableIterInit(&l_tvi, "lineitem"); @tableIterAdvance(&l_tvi); ) {
        var vec = @tableIterGetVPI(&l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, execCtx, vec);) {
                var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))

                // Build AHT
                var agg_input : AggValues2
                agg_input.c_name = join_row3.c_name
                agg_input.c_custkey = join_row3.c_custkey
                agg_input.o_orderkey = join_row3.o_orderkey
                agg_input.o_orderdate = join_row3.o_orderdate
                agg_input.o_totalprice = join_row3.o_totalprice
                agg_input.sum_quantity = @vpiGetReal(vec, 4) // l_quantity
                var agg_hash_val = @hash(agg_input.c_name, agg_input.c_custkey, agg_input.o_orderkey, agg_input.o_totalprice)
                var agg_payload = @ptrCast(*AggPayload2, @aggHTLookup(&state.agg_table2, agg_hash_val, checkAggKey2, &agg_input))
                if (agg_payload == nil) {
                    agg_payload = @ptrCast(*AggPayload2, @aggHTInsert(&state.agg_table2, agg_hash_val))
                    agg_payload.c_name = agg_input.c_name
                    agg_payload.c_custkey = agg_input.c_custkey
                    agg_payload.o_orderkey = agg_input.o_orderkey
                    agg_payload.o_orderdate = agg_input.o_orderdate
                    agg_payload.o_totalprice = agg_input.o_totalprice
                    @aggInit(&agg_payload.sum_quantity)
                }
                @aggAdvance(&agg_payload.sum_quantity, &agg_input.sum_quantity)
            }
        }
    }
    @tableIterClose(&l_tvi)
}

// Scan AHT2, sort
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
    var agg_ht_iter: AHTIterator
    var agg_iter = &agg_ht_iter
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(agg_iter, &state.agg_table2); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload2, @aggHTIterGetRow(agg_iter))

        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsertTopK(&state.sorter, 100))
        sorter_row.c_name = agg_payload.c_name
        sorter_row.c_custkey = agg_payload.c_custkey
        sorter_row.o_orderkey = agg_payload.o_orderkey
        sorter_row.o_orderdate = agg_payload.o_orderdate
        sorter_row.o_totalprice = agg_payload.o_totalprice
        sorter_row.sum_quantity = @aggResult(&agg_payload.sum_quantity)
        @sorterInsertTopKFinish(&state.sorter, 100)
    }
    @aggHTIterClose(agg_iter)

    // Sort
    @sorterSort(&state.sorter)
}

// Iterate through sorter, output
fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter);
         @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {

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
