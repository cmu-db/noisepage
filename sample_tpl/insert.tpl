struct row_struct {
  colA: Integer
}

fun main() -> int {
  // expect no results
  var oldcnt = 0
  for (row1 in empty_table) {
    oldcnt = oldcnt + 1
    var out1 = @ptrCast(*row_struct, @outputAlloc())
    out1.colA = row1.colA
    @outputAdvance()
  }

  var ins : row_struct
  ins.colA = 15
  @insert(1, 2, &ins)
  ins.colA = 721
  @insert(1, 2, &ins)

  // expect two results
  var cnt = 0
  for (row2 in empty_table) {
    cnt = cnt + 1
    var out2 = @ptrCast(*row_struct, @outputAlloc())
    out2.colA = row2.colA
    @outputAdvance()
  }
  @outputFinalize()

  return cnt
}