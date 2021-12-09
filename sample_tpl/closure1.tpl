// Expected output: 3

fun main() -> int32 {
    var x = 1
    // Closure that uses capture in computation;
    // the closure does not write captured variable
    var addValue = lambda [x] (y: int32) -> int32 {
                    return x + y           
                   }
    return addValue(2)
}