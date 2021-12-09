// Expected output: 10

struct S {
    a: int
    b: int
    c: (int32) -> int32
}
struct SDup {
    d: int
    e: int
    f: int
}

fun sss(x : int32) -> int32 {
    return x
}

fun main() -> int {
    var p: S
    p.c = sss
}
