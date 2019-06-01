struct outputStruct {
    out: Integer
}

struct State {
    sum: IntegerSumAggregate
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggInit(&state.sum)
}


fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 1 (hashing)
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
        var pci = @tableIterGetPCI(&tvi)
        for (; @pciHasNext(pci); @pciAdvance(pci)) {
            // TODO: Figure out lineitem column order in storage layer
            var input = @pciGetInt(pci, 0) * @pciGetInt(pci, 1)
            @aggAdvance(&state.sum, &input)
        }
    }

    // Pipeline 2 (Output to upper layers)
    var out : *outputStruct
    out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
    out.out = @aggResult(&state.sum)
    @outputAdvance(execCtx)
    @outputFinalize(execCtx)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return 37
}