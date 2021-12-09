// Expected output: 2

fun main() -> int32 {
    var x = 1
    // Closure that writes to the captured variable
    var addOne = lambda [x] () -> nil {
                      x = x + 1           
                   }
    addOne()
    return x
}