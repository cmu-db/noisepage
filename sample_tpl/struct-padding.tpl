struct State {
    count  : uint32
    sorter : Sorter
    count1 : uint32
    count2 : int32
}
fun sizers(a : uint32, b : Sorter, c : uint32, d: ExecutionContext, e : State) -> nil {}
fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    state.count1 = 42
}
fun tearDownState(state: *State) -> nil {
}
fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    state.count2 = 500
    setUpState(execCtx, &state)
    tearDownState(&state)
    return state.count2
}
