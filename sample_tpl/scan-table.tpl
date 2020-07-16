// Perform:
// select colA from test_1 LIMIT 2;
//
// Should output 500 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  oids[0] = 1 // colA
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  var num_tuples = 0
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
     if (num_tuples < 2) {
        var cola = @pciGetInt(pci, 0)
        ret = ret + 1
      }
      num_tuples = num_tuples + 1
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
