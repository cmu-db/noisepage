// Expected output: 46

fun main() -> int {
    // test
    var x = 0
    // another test
    for (var i = 0; i < 10; i = i + 1) {
        // last
        x = x + i
        /* blah */
    }
    /*
    var a = 0
    var b = 20
    var c = 20
    for (var i = 0; i < 10; i=i+1) {
        a = a + b + c + i
    }
    */
    return x + 1
}
