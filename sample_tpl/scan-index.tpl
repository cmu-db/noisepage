// Perform(using an index):
//
// SELECT colA, colB from test_1 WHERE colA = 500
//
// Should return 1 (number of matching tuples)
// Should also output "500, 9" to std out (the output tuple). The "9" is non deterministic.

struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main(execCtx: *ExecutionContext) -> int64 {
  // Number of output structs (should be 1)
  var count = 0
  // output variable
  var out : *output_struct
  // Index iterator
  var index : IndexIterator
  var col_oids: [2]uint32
  col_oids[0] = 1 // colA
  col_oids[1] = 2 // colB
  @indexIteratorInitBind(&index, execCtx, "test_1", "index_1", col_oids)
  @indexIteratorSetKeyInt(&index, 0, @intToSql(500)) // Set colA
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetInt(&index, 0)
    out.colB = @indexIteratorGetInt(&index, 1)
    count = count + 1
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return count
}