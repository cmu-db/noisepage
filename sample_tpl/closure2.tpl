// Expected output: 6

fun main() -> int32 {
    var x = 1
    var y = 2
    // Closure that uses multiple captures in computation
    var addValues = lambda [x, y] (z: int32) -> int32 {
                    return x + y + z           
                   }
    return addValues(3)
}