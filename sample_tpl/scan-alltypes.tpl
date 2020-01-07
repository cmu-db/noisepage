// Perform:
// SELECT COUNT(*) FROM all_types
//  WHERE bool_col = true
//    AND (tinyint_col >= 0 OR smallint_col >= 0 OR int_col >=0 OR bigint_col >= 0)
//
// Should output 500 because half of the bool_col values will be true
// The goal of this test is to make sure that we support all of the primitive
// types in the TPL language.

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [2]uint32
  // Setup #1 - Read the bool first then tinyint -- This works
  oids[0] = 1 // bool_col
  oids[1] = 2 // tinyint_col
  
  // Setup #2 - Read the tinyint first then bool -- This doesn't work?
  // oids[0] = 2 // tinyint_col
  // oids[1] = 1 // bool_col
  
  // Setup #3 - Read bool first then smallint -- This doesn't work?
  // oids[0] = 1 // bool_col
  // oids[1] = 3 // smallint_col

  @tableIterInitBind(&tvi, execCtx, "all_types", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
        // Setup #1
        var col0 = @pciGetBool(pci, 0)
        var col1 = @pciGetTinyInt(pci, 1)

        // Setup #2
        // bool occurs before tinyint in the original table so is first even though they are the same size so is first in the projection list as well
        // var col0 = @pciGetBool(pci, 0)
        // var col1 = @pciGetTinyInt(pci, 1)

        // Setup #3 // smallint is larger than bool so occurs first in the projection list
        // var col0 = @pciGetBool(pci, 1)
        // var col1 = @pciGetSmallInt(pci, 0)

      if (col0 == true) { 
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

