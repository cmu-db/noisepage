struct OutputStruct {
    revenue : Real
}

struct State {
    join_table : JoinHashTable
    revenue    : RealSumAggregate
    count      : int32  // debug
}

struct JoinRow {
    p_partkey   : Integer
    p_brand     : StringVal
    p_container : StringVal
    p_size      : Integer
}

fun checkJoinKey(execCtx: *ExecutionContext, probe: *VectorProjectionIterator, build: *JoinRow) -> bool {
    if (@vpiGetInt(probe, 1) != build.p_partkey) {
        return false
    }
    return true
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow))
    @aggInit(&state.revenue)
    state.count = 0
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table)
}

// Scan part, build JHT
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var p_tvi : TableVectorIterator
    @tableIterInit(&p_tvi, "part")
    for (@tableIterAdvance(&p_tvi)) {
        var vec = @tableIterGetVPI(&p_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // p_partkey
            var build_row = @ptrCast(*JoinRow, @joinHTInsert(&state.join_table, hash_val))
            build_row.p_partkey = @vpiGetInt(vec, 0) // p_partkey
            build_row.p_brand = @vpiGetString(vec, 3) // p_brand
            build_row.p_container = @vpiGetString(vec, 6) // p_container
            build_row.p_size = @vpiGetInt(vec, 5) // p_size
        }
    }
    @tableIterClose(&p_tvi)

    // Build JHT
    @joinHTBuild(&state.join_table)
}

// Scan lineitem, probe JHT, advance AGG
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {

    // Used for predicates
    var brand12 = @stringToSql("Brand#12")
    var brand23 = @stringToSql("Brand#23")
    var brand34 = @stringToSql("Brand#34")
    var sm_container1 = @stringToSql("SM CASE")
    var sm_container2 = @stringToSql("SM BOX")
    var sm_container3 = @stringToSql("SM PACK")
    var sm_container4 = @stringToSql("SM PKG")
    var med_container1 = @stringToSql("MED BAG")
    var med_container2 = @stringToSql("MED BOX")
    var med_container3 = @stringToSql("MED PKG")
    var med_container4 = @stringToSql("MED PACK")
    var lg_container1 = @stringToSql("LG CASE")
    var lg_container2 = @stringToSql("LG BOX")
    var lg_container3 = @stringToSql("LG PACK")
    var lg_container4 = @stringToSql("LG PKG")
    var mode1 = @stringToSql("AIR")
    var mode2 = @stringToSql("AIR REG")
    var instruct = @stringToSql("DELIVER IN PERSON")

    var l_tvi : TableVectorIterator

    for (@tableIterInit(&l_tvi, "lineitem"); @tableIterAdvance(&l_tvi); ) {
        var vec = @tableIterGetVPI(&l_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1)) // l_partkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey, execCtx, vec);) {
                var join_row = @ptrCast(*JoinRow, @htEntryIterGetRow(&hti))
                if (
                    (
                        (join_row.p_brand == brand12)
                        and (@vpiGetReal(vec, 4) >= 1.0 and @vpiGetReal(vec, 4) <= 11.0)
                        and (join_row.p_size >= 1 and join_row.p_size <= 5)
                        and (join_row.p_container == sm_container1 or join_row.p_container == sm_container2 or join_row.p_container == sm_container3 or join_row.p_container == sm_container4)
                        and (@vpiGetString(vec, 14) == mode1 or @vpiGetString(vec, 14) == mode2)
                        and (@vpiGetString(vec, 13) == instruct)
                    ) or (
                        (join_row.p_brand == brand23)
                        and (@vpiGetReal(vec, 4) >= 10.0 and @vpiGetReal(vec, 4) <= 20.0)
                        and (join_row.p_size >= 1 and join_row.p_size <= 10)
                        and (join_row.p_container == med_container1 or join_row.p_container == med_container2 or join_row.p_container == med_container3 or join_row.p_container == med_container4)
                        and (@vpiGetString(vec, 14) == mode1 or @vpiGetString(vec, 14) == mode2)
                        and (@vpiGetString(vec, 13) == instruct)
                    ) or (
                        (join_row.p_brand == brand34)
                        and (@vpiGetReal(vec, 4) >= 20.0 and @vpiGetReal(vec, 4) <= 30.0)
                        and (join_row.p_size >= 1 and join_row.p_size <= 15)
                        and (join_row.p_container == lg_container1 or join_row.p_container == lg_container2 or join_row.p_container == lg_container3 or join_row.p_container == lg_container4)
                        and (@vpiGetString(vec, 14) == mode1 or @vpiGetString(vec, 14) == mode2)
                        and (@vpiGetString(vec, 13) == instruct)
                    )) {
                    var input = @vpiGetReal(vec, 5) * (1.0 - @vpiGetReal(vec, 6))
                    @aggAdvance(&state.revenue, &input)
                    state.count = state.count + 1
                }
            }
        }
    }
    @tableIterClose(&l_tvi)
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    var out = @ptrCast(*OutputStruct, @resultBufferAllocRow(execCtx))
    out.revenue = @aggResult(&state.revenue)
    @resultBufferFinalize(execCtx)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    pipeline1(execCtx, state)
    pipeline2(execCtx, state)
    pipeline3(execCtx, state)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(execCtx, &state)

    return state.count
}
