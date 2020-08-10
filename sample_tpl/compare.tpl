// Expected output: 200

struct S {
    a: int32
    b: int32
}

fun compare_a_lt_b(a: *S, b: *S) -> bool {
    return a.a < b.a
}

fun main() -> int32 {
    var s1: S
    var s2: S
    s1.a = 100
    s2.a = 10
    if (compare_a_lt_b(&s1, &s2)) {
        return 100
    } else {
        return 200
    }
}
