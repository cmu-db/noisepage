// Expected output: 110

// Fill the input array with 'n' multiples of two
fun fillWithMultiplesOfTwo(arr: [*]int32, n: int32) -> nil {
    for (var i = 0; i < n; i = i + 1) {
        arr[i] = (i + 1) * 2
    }
}

// Sum all elements in the array
fun sumElems(arr: [*]int32, n: int32) -> int32 {
    var ret = 0
    for (var i = 0; i < n; i = i + 1) {
        ret = ret + arr[i]
    }
    return ret
}

fun main() -> int32 {
    var arr: [10]int32
    // Fill array with multiples of two
    fillWithMultiplesOfTwo(&arr, 10)
    // arr = {2, 4, 6, 8, 10, 12, 14, 16, 18, 20), thus sum = 110
    return sumElems(&arr, 10)
}
