struct Output {
    supp_nation : StringVal
    cust_nation : StringVal
    l_year      : Integer
    volume      : Real
}

struct JoinRow1 {
    n1_nationkey : Integer
    n2_nationkey : Integer
    n1_name      : StringVal
    n2_name      : StringVal
}

struct JoinRow2 {
    n1_nationkey : Integer
    n1_name      : StringVal
    n2_name      : StringVal
    c_custkey    : Integer
}

struct JoinRow3 {
    n1_nationkey : Integer
    n1_name      : StringVal
    n2_name      : StringVal
    o_orderkey   : Integer
}

struct JoinRow4 {
    s_suppkey   : Integer
    s_nationkey : Integer
}

struct JoinProbe4 {
    n1_nationkey : Integer
    l_suppkey    : Integer
}

struct AggPayload {
    supp_nation : StringVal
    cust_nation : StringVal
    l_year      : Integer
    volume      : RealSumAggregate
}

struct AggValues {
    supp_nation : StringVal
    cust_nation : StringVal
    l_year      : Integer
    volume      : Real
}

struct SorterRow {
    supp_nation : StringVal
    cust_nation : StringVal
    l_year      : Integer
    volume      : Real
}

struct State {
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    join_table3 : JoinHashTable
    join_table4 : JoinHashTable
    agg_table   : AggregationHashTable
    sorter      : Sorter
    count       : int32 // Debug
}

// Check that the aggregate key already exists
fun checkAggKey(payload: *AggPayload, row: *AggValues) -> bool {
    if (payload.l_year != row.l_year) {
        return false
    }
    if (payload.supp_nation != row.supp_nation) {
        return false
    }
    if (payload.cust_nation != row.cust_nation) {
        return false
    }
    return true
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.supp_nation < rhs.supp_nation) {
        return -1
    }
    if (lhs.supp_nation > rhs.supp_nation) {
        return 1
    }
    if (lhs.cust_nation < rhs.cust_nation) {
        return -1
    }
    if (lhs.cust_nation > rhs.cust_nation) {
        return 1
    }
    if (lhs.l_year < rhs.l_year) {
        return -1
    }
    if (lhs.l_year > rhs.l_year) {
        return 1
    }
    return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
    @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
    @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow4))
    @aggHTInit(&state.agg_table, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @joinHTFree(&state.join_table3)
    @joinHTFree(&state.join_table4)
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // check c_nationkey == n2_nationkey
    if (@vpiGetInt(probe, 3) != build.n2_nationkey) {
        return false
    }
    return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
    // o_custkey == c_custkey
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

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *JoinProbe4, build: *JoinRow4) -> bool {
    // l_suppkey == s_suppkey
    if (probe.l_suppkey != build.s_suppkey) {
        return false
    }
    // n1_nationkey == s_nationkey
    if (probe.n1_nationkey != build.s_nationkey) {
        return false
    }
    return true
}

// BNL nation with nation, then build JHT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var n1_tvi : TableVectorIterator
    var n2_tvi : TableVectorIterator
    var france = @stringToSql("FRANCE")
    var germany = @stringToSql("GERMANY")
    // Step 1: Scan nation1
    for (@tableIterInit(&n1_tvi, "nation"); @tableIterAdvance(&n1_tvi); ) {
        var vec1 = @tableIterGetVPI(&n1_tvi)
        for (; @vpiHasNext(vec1); @vpiAdvance(vec1)) {
            // n_name
            if (@vpiGetString(vec1, 1) == france or @vpiGetString(vec1, 1) == germany) {
                // Step 2: Scan nation2
                for (@tableIterInit(&n2_tvi, "nation"); @tableIterAdvance(&n2_tvi); ) {
                    var vec2 = @tableIterGetVPI(&n2_tvi)
                    for (; @vpiHasNext(vec2); @vpiAdvance(vec2)) {
                        if ((@vpiGetString(vec1, 1) == france and @vpiGetString(vec2, 1) == germany) or @vpiGetString(vec1, 1) == germany and @vpiGetString(vec2, 1) == france) {
                            // Build JHT1
                            var hash_val = @hash(@vpiGetInt(vec2, 0)) // n2_nationkey
                            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
                            build_row1.n1_nationkey = @vpiGetInt(vec1, 0) // n1_nationkey
                            build_row1.n2_nationkey = @vpiGetInt(vec2, 0) // n2_nationkey
                            build_row1.n1_name = @vpiGetString(vec1, 1) // n1_name
                            build_row1.n2_name = @vpiGetString(vec2, 1) // n2_name
                        }
                    }
                }
            @tableIterClose(&n2_tvi)
            }
        }
    }
    @tableIterClose(&n1_tvi)

    // Build table
    @joinHTBuild(&state.join_table1)
}

// Scan Customer, probe JHT1, build JHT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Step1: Scan customer
    var c_tvi : TableVectorIterator
    for (@tableIterInit(&c_tvi, "customer"); @tableIterAdvance(&c_tvi); ) {
        var vec = @tableIterGetVPI(&c_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe JHT1
            var hash_val = @hash(@vpiGetInt(vec, 3)) // c_nationkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
                var join_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

                // Step 3: Insert into JHT2
                var hash_val2 = @hash(@vpiGetInt(vec, 0)) // c_custkey
                var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val2))
                build_row2.n1_nationkey = join_row1.n1_nationkey
                build_row2.n1_name = join_row1.n1_name
                build_row2.n2_name = join_row1.n2_name
                build_row2.c_custkey = @vpiGetInt(vec, 0) // c_custkey
            }
        }
    }
    @tableIterClose(&c_tvi)

    // Build table
    @joinHTBuild(&state.join_table2)
}

// Scan orders, probe JHT2, build JHT3
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Step 1: Sequential Scan
    var o_tvi : TableVectorIterator
    for (@tableIterInit(&o_tvi, "orders"); @tableIterAdvance(&o_tvi); ) {
        var vec = @tableIterGetVPI(&o_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe JHT2
            var hash_val = @hash(@vpiGetInt(vec, 1)) // o_custkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
                var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))

                // Step 3: Insert into join table 3
                var hash_val3 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
                var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table3, hash_val3))
                build_row3.n1_nationkey = join_row2.n1_nationkey
                build_row3.n1_name = join_row2.n1_name
                build_row3.n2_name = join_row2.n2_name
                build_row3.o_orderkey = @vpiGetInt(vec, 0)
            }
        }
    }
    @tableIterClose(&o_tvi)

    // Build table
    @joinHTBuild(&state.join_table3)
}

// Scan supplier, build JHT4
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var s_tvi : TableVectorIterator
    for (@tableIterInit(&s_tvi, "supplier"); @tableIterAdvance(&s_tvi); ) {
        var vec = @tableIterGetVPI(&s_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0), @vpiGetInt(vec, 3)) // s_suppkey, s_nationkey
            var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&state.join_table4, hash_val))
            build_row4.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
            build_row4.s_nationkey = @vpiGetInt(vec, 3) // s_nationkey
        }
    }
    @tableIterClose(&s_tvi)

    // Build table
    @joinHTBuild(&state.join_table4)
}

fun p5_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_shipdate
    @filterGe(execCtx, vector_proj, 10, @dateToSql(1995, 1, 1), tids)
}

fun p5_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // l_shipdate
    @filterLe(execCtx, vector_proj, 10, @dateToSql(1996, 12, 31), tids)
}

// Scan lineitem, probe JHT3, probe JHT4, build AHT
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, p5_filter_clause0term0, p5_filter_clause0term1)
    @filterManagerFinalize(&filter)

    var l_tvi : TableVectorIterator
    for (@tableIterInit(&l_tvi, "lineitem"); @tableIterAdvance(&l_tvi); ) {
        var vec = @tableIterGetVPI(&l_tvi)

        // Filter
        @filterManagerRunFilters(&filter, vec, execCtx)

        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            // Step 2: Probe JHT3
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var hti3: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table3, &hti3, hash_val); @htEntryIterHasNext(&hti3, checkJoinKey3, execCtx, vec);) {
                var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti3))

                // Step 3: Probe JHT4
                var hash_val4 = @hash(@vpiGetInt(vec, 2), join_row3.n1_nationkey) // l_suppkey
                var join_probe4 : JoinProbe4 // Materialize the right pipeline
                join_probe4.l_suppkey = @vpiGetInt(vec, 2)
                join_probe4.n1_nationkey = join_row3.n1_nationkey
                var hti4: HashTableEntryIterator
                for (@joinHTLookup(&state.join_table4, &hti4, hash_val4); @htEntryIterHasNext(&hti4, checkJoinKey4, execCtx, &join_probe4);) {
                    var join_row4 = @ptrCast(*JoinRow4, @htEntryIterGetRow(&hti4))

                    // Step 4: Build Agg HT
                    var agg_input : AggValues // Materialize
                    agg_input.supp_nation = join_row3.n1_name
                    agg_input.cust_nation = join_row3.n2_name
                    agg_input.l_year = @extractYear(@vpiGetDate(vec, 10))
                    agg_input.volume = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6)) // l_extendedprice * (1.0 -  l_discount)
                    var agg_hash_val = @hash(agg_input.supp_nation, agg_input.cust_nation, agg_input.l_year)
                    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, &agg_input))
                    if (agg_payload == nil) {
                        agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
                        agg_payload.supp_nation = agg_input.supp_nation
                        agg_payload.cust_nation = agg_input.cust_nation
                        agg_payload.l_year = agg_input.l_year
                        @aggInit(&agg_payload.volume)
                    }
                    @aggAdvance(&agg_payload.volume, &agg_input.volume)
                }
            }
        }
    }
    // Build table
    @tableIterClose(&l_tvi)

    // Cleanup
    @filterManagerFree(&filter)
}

// Scan AHT, sort
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
    var agg_ht_iter: AHTIterator
    var agg_iter = &agg_ht_iter

    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
        sorter_row.supp_nation = agg_payload.supp_nation
        sorter_row.cust_nation = agg_payload.cust_nation
        sorter_row.l_year = agg_payload.l_year
        sorter_row.volume = @aggResult(&agg_payload.volume)
    }
    @aggHTIterClose(agg_iter)

    // Sort
    @sorterSort(&state.sorter)
}

fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var out = @ptrCast(*Output, @resultBufferAllocRow(execCtx))

        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.supp_nation = sorter_row.supp_nation
        out.cust_nation = sorter_row.cust_nation
        out.l_year = sorter_row.l_year
        out.volume = sorter_row.volume
        state.count = state.count + 1
    }
    @sorterIterClose(&sort_iter)

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
    teardownState(execCtx, &state)
    return state.count
}
