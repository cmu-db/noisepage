// Expected output: 10
// SQL: SELECT col_b, count(col_a) FROM test_1 GROUP BY col_b

struct State {
    table: AggregationHashTable
    count: int32
}

struct Agg {
    key: Integer
    count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    @aggHTInit(&state.table, execCtx, @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
    @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, vpi: *VectorProjectionIterator) -> bool {
    var key = @vpiGetInt(vpi, 1)
    return @sqlToBool(key == agg.key)
}

fun constructAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    agg.key = @vpiGetInt(vpi, 1)
    @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    var input = @vpiGetInt(vpi, 0)
    @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
    var ht = &state.table
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [2]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
            var hash_val = @hash(@vpiGetInt(vec, 1))
            var agg = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
            if (agg == nil) {
                agg = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
                constructAgg(agg, vec)
            } else {
                updateAgg(agg, vec)
            }
        }
    }
    @tableIterClose(&tvi)
}

fun pipeline_2(state: *State) -> nil {
    var aht_iter: AHTIterator
    var iter = &aht_iter
    for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
        var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
        state.count = state.count + 1
    }
    @aggHTIterClose(iter)
}

fun execQuery(execCtx: *ExecutionContext, qs: *State) -> nil {
    pipeline_1(execCtx, qs)
    pipeline_2(qs)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    tearDownState(&state)

    var ret = state.count
    return ret
}
