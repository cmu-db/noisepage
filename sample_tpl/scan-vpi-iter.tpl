// Perform a vectorized scan for:
//
// SELECT * FROM test_1 WHERE cola < 500
//
// Should return 500 (number of output rows)


fun Lt500(pci: *ProjectedColumnsIterator) -> int32 {
  var param: Integer = @intToSql(500)
  var cola: Integer
  if (@pciIsFiltered(pci)) {
    for (; @pciHasNextFiltered(pci); @pciAdvanceFiltered(pci)) {
      cola = @pciGetInt(pci, 0)
      @pciMatch(pci, cola < param)
    }
  } else {
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      cola = @pciGetInt(pci, 0)
      @pciMatch(pci, cola < param)
    }
  }
  @pciResetFiltered(pci)
  return 0
}

fun Lt500_Vec(pci: *ProjectedColumnsIterator) -> int32 {
  return @filterLt(pci, 0, 4, 500)
}

fun count(pci: *ProjectedColumnsIterator) -> int32 {
  var ret = 0
  if (@pciIsFiltered(pci)) {
    for (; @pciHasNextFiltered(pci); @pciAdvanceFiltered(pci)) {
      ret = ret + 1
    }
  } else {
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      ret = ret + 1
    }
  }
  @pciResetFiltered(pci)
  return ret
}

fun main(execCtx: *ExecutionContext) -> int {
  var ret :int = 0

  var filter: FilterManager
  @filterManagerInit(&filter)
  @filterManagerInsertFilter(&filter, Lt500, Lt500_Vec)
  @filterManagerFinalize(&filter)

  var tvi: TableVectorIterator
  var col_oids : [1]uint32
  col_oids[0] = 1
  for (@tableIterInitBind(&tvi, execCtx, "test_1", col_oids); @tableIterAdvance(&tvi); ) {
    var pci = @tableIterGetPCI(&tvi)
    @filtersRun(&filter, pci)
    ret = ret + count(pci)
  }

  @filterManagerFree(&filter)
  @tableIterClose(&tvi)
  return ret
}
