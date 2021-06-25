// Expected output: 3

fun main(exec : *ExecutionContext) -> int32 {
    var x = 1
    var addValue = lambda [x] (y: int32) -> int32 {
                    x = x + y           
                   }
    addValue(2)
    return x
}