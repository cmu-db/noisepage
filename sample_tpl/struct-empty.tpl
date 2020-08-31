// Expected output: 0

struct Empty1 { }

struct Empty2 { arr: [0]int8 }

struct Empty3 { arr: [0]int32 }

fun something1(s: *Empty1) -> void {}

fun something2(s: *Empty2) -> void {}

fun something3(s: *Empty3) -> void {}

fun main() -> int32 {
    var e1: Empty1
    var e2: Empty2
    var e3: Empty3

    something1(&e1)
    something2(&e2)
    something3(&e3)

    return 0
}
