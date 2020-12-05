// Expected output: 10

fun i16() -> int16 {
    var a: int32 = 1
    var b: uint16 = 2
    return @intCast(int16, a) + @intCast(int16, b)
}

fun u8() -> uint8 {
    var a: uint8 = 3
    var b: uint8 = 4
    return a + b
}

fun add(a: uint8, b : int16) -> uint32 {
    return @intCast(uint32, a) + @intCast(uint32, b)
}

fun gg() -> int8 { return 1 }

fun hh() -> int16 { return 4 }

fun main() -> int32 {
    var a = i16()
    var b = u8()
    var c: int8 = 44
    var d: int16 = hh() + 10
    var e: bool = d < 4
    // a = 3
    // b = 7
    // return 10
    return @intCast(int, add(b, a))
}
