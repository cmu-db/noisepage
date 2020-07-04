struct OutputStruct {
    revenue : Real
}

struct State {
    join_table : JoinHashTable
    revenue    : RealSumAggregate
    count      : int32 // Debug
}

struct P1_ThreadState {
    ts_join_table : JoinHashTable
}

struct P2_ThreadState {
    ts_revenue : RealSumAggregate
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
    state.count = 0
    @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow))
    @aggInit(&state.revenue)
}

fun tearDownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @joinHTFree(&state.join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 1 State
// -----------------------------------------------------------------------------

fun p1_initThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @joinHTInit(&ts.ts_join_table, @execCtxGetMem(execCtx), @sizeOf(JoinRow))
}

fun p1_tearDownThreadState(execCtx: *ExecutionContext, ts: *P1_ThreadState) -> nil {
    @joinHTFree(&ts.ts_join_table)
}

// -----------------------------------------------------------------------------
// Pipeline 2 State
// -----------------------------------------------------------------------------

fun p2_initThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil {
    @aggInit(&ts.ts_revenue)
}

fun p2_tearDownThreadState(execCtx: *ExecutionContext, ts: *P2_ThreadState) -> nil { }

// -----------------------------------------------------------------------------
// Pipeline 1
// -----------------------------------------------------------------------------

// Scan part, build JHT
fun p1_worker(state: *State, ts: *P1_ThreadState, p_tvi: *TableVectorIterator) -> nil {
    var x = 0
    for (@tableIterAdvance(p_tvi)) {
        var vec = @tableIterGetVPI(p_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 0)) // p_partkey
            var build_row = @ptrCast(*JoinRow, @joinHTInsert(&ts.ts_join_table, hash_val))
            build_row.p_partkey = @vpiGetInt(vec, 0) // p_partkey
            build_row.p_brand = @vpiGetString(vec, 3) // p_brand
            build_row.p_container = @vpiGetString(vec, 6) // p_container
            build_row.p_size = @vpiGetInt(vec, 5) // p_size
        }
    }
}

fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P1_ThreadState), p1_initThreadState, p1_tearDownThreadState, execCtx)

    @iterateTableParallel("part", state, &tls, p1_worker)

    var off: uint32 = 0
    @joinHTBuildParallel(&state.join_table, &tls, off)

    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 2
// -----------------------------------------------------------------------------

// Scan lineitem, probe JHT, advance AGG
fun p2_worker(state: *State, ts: *P2_ThreadState, p_tvi: *TableVectorIterator) -> nil {
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
    for (@tableIterAdvance(p_tvi)) {
        var vec = @tableIterGetVPI(p_tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1)) // l_partkey
            var hti: HashTableEntryIterator
            for (@joinHTLookup(&state.join_table, &hti, hash_val); @htEntryIterHasNext(&hti, checkJoinKey, state, vec);) {
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
                    @aggAdvance(&ts.ts_revenue, &input)
                }
            }
        }
    }
}

fun gatherAgg(qs: *State, ts: *P2_ThreadState) -> nil { @aggMerge(&qs.revenue, &ts.ts_revenue) }

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
    var tls : ThreadStateContainer
    @tlsInit(&tls, @execCtxGetMem(execCtx))
    @tlsReset(&tls, @sizeOf(P2_ThreadState), p2_initThreadState, p2_tearDownThreadState, execCtx)

    @iterateTableParallel("lineitem", state, &tls, p2_worker)
    @tlsIterate(&tls, state, gatherAgg)

    @tlsFree(&tls)
}

// -----------------------------------------------------------------------------
// Pipeline 3
// -----------------------------------------------------------------------------

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
