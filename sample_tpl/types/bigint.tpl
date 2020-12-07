// Expected output: 0

fun main() -> int {
  var x = @intToSql(4398046511104)

  // Test that it isn't parsing to 0.
  if (x == @intToSql(0)) {
    return -1
  }

  return 0
}
