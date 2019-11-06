// Perform:
// SELECT COUNT(*) FROM all_types
//  WHERE colA >= 0 OR colB >= 0 OR colC >=0 OR colD >= 0
//
// Should output 1000 (number of output rows).
// The goal of this test is to make sure that we support all of the primitive
// types in the TPL language.

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [4]uint32
  oids[0] = 1 // tinyint_col
  oids[1] = 2 // smallint_col
  oids[2] = 3 // int_col
  oids[3] = 4 // bigint_col

  @tableIterInitBind(&tvi, execCtx, "all_types", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var col0 = @pciGetTinyInt(pci, 0)
      var col1 = @pciGetSmallInt(pci, 1)
      var col2 = @pciGetInt(pci, 2)
      var col3 = @pciGetBigInt(pci, 3)

      if (col0 >= 0 or col1 >= 0 or col2 >= 0 or col3 >= 0) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}

