struct OutputStruct {
    p_brand      : StringVal
    p_type       : StringVal
    p_size       : Integer
    supplier_cnt : Integer
}

struct State {
    join_table1 : JoinHashTable
    join_table2 : JoinHashTable
    agg_table   : AggregationHashTable
    sorter      : Sorter
    count       : int32 // Debug
}

struct JoinRow1 {
    p_brand   : StringVal
    p_type    : StringVal
    p_size    : Integer
    p_partkey : Integer
}

struct JoinRow2 {
    s_suppkey : Integer
}

struct AggValues {
    p_brand      : StringVal
    p_type       : StringVal
    p_size       : Integer
    supplier_cnt : Integer
}

struct AggPayload {
    p_brand      : StringVal
    p_type       : StringVal
    p_size       : Integer
    supplier_cnt : CountAggregate // TODO(Amadou): Replace by count disctinct aggregate
}

struct SorterRow {
    p_brand      : StringVal
    p_type       : StringVal
    p_size       : Integer
    supplier_cnt : Integer
}


fun checkJoinKey1(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow1) -> bool {
    // ps_partkey == p_partkey
    if (@vpiGetInt(probe, 0) != build.p_partkey) {
        return false
    }
    return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow2) -> bool {
    // ps_suppkey == s_suppkey
    if (@vpiGetInt(probe, 1) != build.s_suppkey) {
        return false
    }
    return true
}

fun checkAggKey(payload: *AggPayload, row: *AggValues) -> bool {
    if (payload.p_size != row.p_size) {
        return false
    }
    if (payload.p_brand != row.p_brand) {
        return false
    }
    if (payload.p_type != row.p_type) {
        return false
    }
    return true
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
    if (lhs.supplier_cnt < rhs.supplier_cnt) {
        return 1 // desc
    }
    if (lhs.supplier_cnt > rhs.supplier_cnt) {
        return -1 // desc
    }
    if (lhs.p_brand < rhs.p_brand) {
        return -1
    }
    if (lhs.p_brand > rhs.p_brand) {
        return 1
    }
    if (lhs.p_type < rhs.p_type) {
        return -1
    }
    if (lhs.p_type > rhs.p_type) {
        return 1
    }
    if (lhs.p_size < rhs.p_size) {
        return -1
    }
    if (lhs.p_size > rhs.p_size) {
        return 1
    }
    return 0
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
    @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
    @aggHTInit(&state.agg_table, execCtx, execCtx, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
    @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table1)
    @joinHTFree(&state.join_table2)
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
}

// Scan part, nuild JHT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var p_tvi : TableVectorIterator
    @tableIterInit(&p_tvi, "part")
    var brand = @stringToSql("Brand#45")
    var pattern = @stringToSql("MEDIUM POLISHED%")
    for (@tableIterAdvance(&p_tvi)) {
        var vec = @tableIterGetVPI(&p_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            if (@vpiGetInt(vec, 5) != 49 // p_size
              and @vpiGetInt(vec, 5) != 14
              and @vpiGetInt(vec, 5) != 23
              and @vpiGetInt(vec, 5) != 45
              and @vpiGetInt(vec, 5) != 19
              and @vpiGetInt(vec, 5) != 3
              and @vpiGetInt(vec, 5) != 36
              and @vpiGetInt(vec, 5) != 9
              and @vpiGetString(vec, 3) != brand // p_brand
              and !(@sqlToBool(@like(@vpiGetString(vec, 4), pattern)))) // p_type
            {
                var hash_val = @hash(@vpiGetInt(vec, 0)) // p_partkey
                var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
                build_row1.p_partkey = @vpiGetInt(vec, 0) // p_partkey
                build_row1.p_brand = @vpiGetString(vec, 3) // p_brand
                build_row1.p_size = @vpiGetInt(vec, 5) // p_size
                build_row1.p_type = @vpiGetString(vec, 4) // p_type
            }
        }
    }
    @tableIterClose(&p_tvi)

    // Build table
    @joinHTBuild(&state.join_table1)
}

// Scan supplier, buil JHT 2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var s_tvi : TableVectorIterator
    @tableIterInit(&s_tvi, "supplier")
    // TODO(Amadou): Replace with %Customer%Complaints%
    var pattern = @stringToSql("%instructions%requests%")
    for (@tableIterAdvance(&s_tvi)) {
        var vec = @tableIterGetVPI(&s_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            if (@like(@vpiGetString(vec, 6), pattern)) {
                var hash_val = @hash(@vpiGetInt(vec, 0)) // s_suppkey
                var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val))
                build_row2.s_suppkey = @vpiGetInt(vec, 0) // s_suppkey
            }
        }
    }
    @tableIterClose(&s_tvi)

    // Build table
    @joinHTBuild(&state.join_table2)
}

// Scan partsupp, probe JHT1, probe JHT2, build AHT
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    var ps_tvi : TableVectorIterator
    @tableIterInit(&ps_tvi, "partsupp")
    for (@tableIterAdvance(&ps_tvi)) {
        var vec = @tableIterGetVPI(&ps_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // ps_partkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table1, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
                var join_row1 = @ptrCast(*JoinRow1, @htEntryIterGetRow(&hti))
                // Anti-Join
                var hash_val2 = @hash(@vpiGetInt(vec, 1)) // ps_suppkey
                var hti2: HashTableEntryIterator
                @joinHTLookup(&state.join_table2, &hti2, hash_val2)
                if (!(@htEntryIterHasNext(&hti2, checkJoinKey2, execCtx, vec))) {
                    state.count = state.count + 1
                    var agg_input : AggValues // Materialize
                    agg_input.p_brand = join_row1.p_brand
                    agg_input.p_type = join_row1.p_type
                    agg_input.p_size = join_row1.p_size
                    agg_input.supplier_cnt = @vpiGetInt(vec, 1) // ps_suppkey
                    var agg_hash_val = @hash(agg_input.p_brand, agg_input.p_type, agg_input.p_size)
                    var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, &agg_input))
                    if (agg_payload == nil) {
                        agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
                        agg_payload.p_brand = agg_input.p_brand
                        agg_payload.p_type = agg_input.p_type
                        agg_payload.p_size = agg_input.p_size
                        @aggInit(&agg_payload.supplier_cnt)
                    }
                    @aggAdvance(&agg_payload.supplier_cnt, &agg_input.supplier_cnt)
                }
            }
        }
    }
    @tableIterClose(&ps_tvi)
}

fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
    var agg_ht_iter: AHTIterator
    var agg_iter = &agg_ht_iter
    // Step 1: Iterate through Agg Hash Table
    for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
        // Step 2: Build Sorter
        var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
        sorter_row.p_brand = agg_payload.p_brand
        sorter_row.p_type = agg_payload.p_type
        sorter_row.p_size = agg_payload.p_size
        sorter_row.supplier_cnt = @aggResult(&agg_payload.supplier_cnt)
    }
    @aggHTIterClose(agg_iter)

    // Sort
    @sorterSort(&state.sorter)
}


// Iterate through sorter, output
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        var out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
        out.p_brand = sorter_row.p_brand
        out.p_type = sorter_row.p_type
        out.p_size = sorter_row.p_size
        out.supplier_cnt = sorter_row.supplier_cnt
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
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
