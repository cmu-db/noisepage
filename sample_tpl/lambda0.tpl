// Expected output: 2

fun main() -> int32 {
    // Lambda without capture
    var addOne = lambda [] (x: int32) -> int32 {
                  return x + 1           
                 }
    return addOne(1)
}