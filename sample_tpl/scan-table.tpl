// Perform:
// select colA from test_1 WHERE colA < 500;
//
// Should output 500 (number of output rows)

fun main(execCtx: *ExecutionContext) -> int64 {
  var ret = 0
  var tvi: TableVectorIterator
  var oids: [1]uint32
  oids[0] = 1 // colA
  @tableIterInitBind(&tvi, execCtx, "test_1", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      var cola = @pciGetInt(pci, 0)
      if (cola < 500) {
        ret = ret + 1
      }
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  return ret
}
