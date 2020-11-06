// Expected output: 15

fun f() -> int32 {
    return 3
}

fun main() -> int32 {
    return f() + f() + f() * f()
}
