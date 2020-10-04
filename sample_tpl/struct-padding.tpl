struct State {
    count  : uint32
    sorter : Sorter
    count1 : uint32
    count2 : uint32
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    state.count1 = 42
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    state.count2 = 500
    setUpState(execCtx, &state)
    return state.count2
}
