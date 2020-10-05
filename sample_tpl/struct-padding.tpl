struct State {
    count  : uint32
    sorter : Sorter
    count1 : uint32
    count2 : uint32
}

struct State2 {
    count  : uint32
    sorter : Sorter
    count1 : [1]uint32
    count2 : uint32
}

struct Embed {
    a : int32
}

struct State3 {
    count  : uint32
    sorter : Sorter
    embed  : Embed
    count2 : uint32
}

fun test1() -> int32 {
    var state: State
    state.count2 = 500
    state.count = 0
    state.count1 = 42
    return state.count2
}

fun test2() -> int32 {
    var state: State2
    state.count2 = 500
    state.count1[0] = 42
    return state.count2
}

fun test3() -> int32 {
    var state: State3
    state.count2 = 500
    state.embed.a = 42
    return state.count2
}

fun main() -> int32 {
    var sum : int32

    // Invokes each of test1(), test2(), and test3().
    // Each function is expected to return 500 on success.
    // A return value of 1500 indicates all 3 functions succeeded.
    sum = test1() + test2() + test3()
    return sum
}
