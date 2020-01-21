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
  
  // These are the columns that we want. The order listed here will be 
  // different than the order that we get back from the projected column.
  var oids: [2]uint32
  oids[0] = 1 // bool_col
  oids[1] = 2 // tinyint_col
//   oids[2] = 3 // smallint_col
//   oids[3] = 4 // int_col
//   oids[4] = 5 // bigint_col

  @tableIterInitBind(&tvi, execCtx, "all_types_table", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
        var col0 = @pciGetBool(pci, 0)
        var col1 = @pciGetTinyInt(pci, 1)
//         var col2 = @pciGetSmallInt(pci, 1)
//         var col3 = @pciGetInt(pci, 2)
//         var col4 = @pciGetBigInt(pci, 4)

        if ((col0 == true) and 
            (col1 >= 0)) { //  or col2 >= 0 or col3 >= 0 or col4 >= 0)) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}

