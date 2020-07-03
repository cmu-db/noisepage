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
    return payload.n_name == row.n_name
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // check n_regionkey == r_regionkey
    return @vpiGetInt(probe, 2) == build.r_regionkey
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

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @joinHTFree(&state.join_table3)
    @joinHTFree(&state.join_table4)
    @joinHTFree(&state.join_table5)
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
}

fun p1_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // r_name
    @filterEq(execCtx, vector_proj, 1, @stringToSql("ASIA"), tids)
}

// Scan Region table and build HT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, p1_filter_clause0term0)
    @filterManagerFinalize(&filter)

    var r_tvi : TableVectorIterator
    for (@tableIterInit(&r_tvi, "region"); @tableIterAdvance(&r_tvi); ) {
        var vec = @tableIterGetVPI(&r_tvi)

        // Step 1: Filter
        @filterManagerRunFilters(&filter, vec, execCtx)

        // Step 2: Insert into table
        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // r_regionkey
            var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
            build_row1.r_regionkey = @vpiGetInt(vec, 0) // r_regionkey
            state.count = state.count + 1
        }
    }
    @tableIterClose(&r_tvi)

    // Step 3: Build HT1
    @joinHTBuild(&state.join_table1)

    // Cleanup
    @filterManagerFree(&filter)
}

// Scan Nation table, probe HT1, build HT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    // Step 1: Sequential Scan
    var n_tvi : TableVectorIterator
    @tableIterInit(&n_tvi, "nation")
    for (@tableIterAdvance(&n_tvi)) {
        var vec = @tableIterGetVPI(&n_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT1
            var hash_val = @hash(@vpiGetInt(vec, 2)) // n_regionkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
                var build_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))

                // Step 3: Insert into join table 2
                var hash_val2 = @hash(@vpiGetInt(vec, 0)) // n_nationkey
                var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val2))
                build_row2.n_nationkey = @vpiGetInt(vec, 0) // n_nationkey
                build_row2.n_name = @vpiGetString(vec, 1) // n_name
            }
        }
    }
    // Step 4: Build HT2
    @joinHTBuild(&state.join_table2)
    @tableIterClose(&n_tvi)
}


// Scan Customer table, probe HT2, build HT3
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    var c_tvi : TableVectorIterator
    @tableIterInit(&c_tvi, "customer")
    for (@tableIterAdvance(&c_tvi)) {
        var vec = @tableIterGetVPI(&c_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT2
            var hash_val = @hash(@vpiGetInt(vec, 3)) // c_nationkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table2, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
                var join_row2 = @ptrCast(*JoinRow2, @htEntryIterGetRow(&hti))
                // Step 3: Insert into join table 3
                var hash_val3 = @hash(@vpiGetInt(vec, 0)) // c_custkey
                var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table3, hash_val3))
                build_row3.n_nationkey = join_row2.n_nationkey
                build_row3.n_name = join_row2.n_name
                build_row3.c_custkey = @vpiGetInt(vec, 0) // c_custkey
            }
        }
    }
    // Step 4: Build HT3
    @joinHTBuild(&state.join_table3)
    @tableIterClose(&c_tvi)
}

fun p4_filter_clause0term0(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // o_orderdate
    @filterGe(execCtx, vector_proj, 4, @dateToSql(1990, 1, 1), tids)
}

fun p4_filter_clause0term1(execCtx: *ExecutionContext, vector_proj: *VectorProjection, tids: *TupleIdList) -> nil {
    // o_orderdate
    @filterLe(execCtx, vector_proj, 4, @dateToSql(2000, 1, 1), tids)
}

// Scan Orders table, probe HT3, build HT4
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var filter: FilterManager
    @filterManagerInit(&filter, execCtx)
    @filterManagerInsertFilter(&filter, p4_filter_clause0term0, p4_filter_clause0term1)
    @filterManagerFinalize(&filter)

    // Step 1: Sequential Scan
    var o_tvi : TableVectorIterator
    @tableIterInit(&o_tvi, "orders")
    for (@tableIterAdvance(&o_tvi)) {
        var vec = @tableIterGetVPI(&o_tvi)

        // Filter
        @filterManagerRunFilters(&filter, vec, execCtx)

        for (; @vpiHasNextFiltered(vec); @vpiAdvanceFiltered(vec)) {
            // Step 2: Probe HT3
            var hash_val = @hash(@vpiGetInt(vec, 1)) // o_custkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table3, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey3, execCtx, vec);) {
                var join_row3 = @ptrCast(*JoinRow3, @htEntryIterGetRow(&hti))
                // Step 3: Insert into join table 4
                var hash_val4 = @hash(@vpiGetInt(vec, 0)) // o_orderkey
                var build_row4 = @ptrCast(*JoinRow4, @joinHTInsert(&state.join_table4, hash_val4))
                build_row4.n_nationkey = join_row3.n_nationkey
                build_row4.n_name = join_row3.n_name
                build_row4.o_orderkey = @vpiGetInt(vec, 0) // o_orderkey
            }
        }
    }
    @tableIterClose(&o_tvi)

    // Step 4: Build HT4
    @joinHTBuild(&state.join_table4)

    // Cleanup
    @filterManagerFree(&filter)
}

// Scan Supplier, build join HT5
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    var s_tvi : TableVectorIterator
    @tableIterInit(&s_tvi, "supplier")
    for (@tableIterAdvance(&s_tvi)) {
        var vec = @tableIterGetVPI(&s_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Insert into HT5
            var hash_val = @hash(@vpiGetInt(vec, 0), @vpiGetInt(vec, 3)) // s_suppkey, s_nationkey
            var build_row5 = @ptrCast(*JoinRow5, @joinHTInsert(&state.join_table5, hash_val))
            build_row5.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
            build_row5.s_nationkey = @vpiGetInt(vec, 3) // s_nationkey
        }
    }

    // Build
    @joinHTBuild(&state.join_table5)
    @tableIterClose(&s_tvi)
}


// Scan Lineitem, probe HT4, probe HT5, build agg HT
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
    var l_tvi : TableVectorIterator
    @tableIterInit(&l_tvi, "lineitem")
    for (@tableIterAdvance(&l_tvi)) {
        var vec = @tableIterGetVPI(&l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            // Step 2: Probe HT4
            var hash_val = @hash(@vpiGetInt(vec, 0)) // l_orderkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table4, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey4, execCtx, vec);) {
                var join_row4 = @ptrCast(*JoinRow4, @htEntryIterGetRow(&hti))

                // Step 3: Probe HT5
                var hash_val5 = @hash(@vpiGetInt(vec, 2), join_row4.n_nationkey) // l_suppkey
                var join_probe5 : JoinProbe5 // Materialize the right pipeline
                join_probe5.n_nationkey = join_row4.n_nationkey
                join_probe5.l_suppkey = @vpiGetInt(vec, 2)
                var hti5: HashTableEntryIterator
                for (@joinHTLookup(&state.join_table5, &hti5, hash_val5); @htEntryIterHasNext(&hti5, checkJoinKey5, execCtx, &join_probe5);) {
                    var join_row5 = @ptrCast(*JoinRow5, @htEntryIterGetRow(&hti5))
                    // Step 4: Build Agg HT
                    var agg_input : AggValues // Materialize
                    agg_input.n_name = join_row4.n_name
                    agg_input.revenue = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6)) // l_extendedprice * (1.0 -  l_discount)
                    var agg_hash_val = @hash(join_row4.n_name)
                    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, &agg_input))
                    if (agg_payload == nil) {
                        agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
                        agg_payload.n_name = agg_input.n_name
                        @aggInit(&agg_payload.revenue)
                    }
                    @aggAdvance(&agg_payload.revenue, &agg_input.revenue)
                }
            }
        }
    }
    @tableIterClose(&l_tvi)
}

// Scan Agg HT table, sort
fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
    var agg_ht_iter: AHTIterator
    var agg_iter = &agg_ht_iter

    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
        sorter_row.n_name = agg_payload.n_name
        sorter_row.revenue = @aggResult(&agg_payload.revenue)
    }
    @sorterSort(&state.sorter)
    @aggHTIterClose(agg_iter)
}

// Scan sorter, output
fun pipeline8(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    var out: *OutputStruct
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))

        // Output
        out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        out.n_name = sorter_row.n_name
        out.revenue = sorter_row.revenue
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
    pipeline8(execCtx, state)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)

    return state.count
}
