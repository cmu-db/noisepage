// Expected output: 110

struct X {
    a: int32
}

struct S {
    x: int32
    xx: X
}

// Fill the input array with multiples of a given N
fun fillWithMultiplesOfN(arr: [*]S, len: int32, N: int32) -> nil {
    for (var i = 0; i < len; i = i + 1) {
        arr[i].xx.a = (i + 1) * N
    }
}

// Sum all elements in the array
fun sumElems(arr: [*]S, len: int32) -> int32 {
    var ret = 0
    for (var i = 0; i < len; i = i + 1) {
        ret = ret + arr[i].xx.a
    }
    return ret
}

fun main() -> int32 {
    var arr: [10]S
    // Fill array with multiples of two
    fillWithMultiplesOfN(&arr, 10, 2)
    // arr = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20), thus sum = 110
    return sumElems(&arr, 10)
}
