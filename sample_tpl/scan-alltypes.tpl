// Perform:
// SELECT COUNT(*) FROM all_types_table
//  WHERE bool_col = true
//    AND (tinyint_col >= 0 OR smallint_col >= 0 OR int_col >=0 OR bigint_col >= 0)
//
// Should output 500 because half of the bool_col values will be true
// The goal of this test is to make sure that we support all of the primitive
// types in the TPL language.

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  // Setup #1 - Read the bool first then tinyint -- This works
  oids[0] = 1 // bool_col

  @tableIterInitBind(&tvi, execCtx, "all_types_table", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
        var col0 = @pciGetBool(pci, 0)
        var col1 = false

        if (col1 == col1) {
          ret = ret + 1
        }
        
        if (col0 == col1) { 
          // and 
          // (col1 >= 0 or col2 >= 0 or col3 >= 0 or col4 >= 0)) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}

