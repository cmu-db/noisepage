// Test scoping

fun main() -> int {
  var a = 2
  if (a < 10) {             // enter
    var b = 8               // a = 2, b = 8
    if (a < b) {            // enter
      var b = 5             // a = 2, b = 5
      if (a < b) {          // enter
        var b = 1           // a = 2, b = 1
        if (a < b) {        // out
          return 100
        }
      }
    }
  }
  return 3                  // return 2
}