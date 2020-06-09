// Perform a vectorized scan for:
//
// SELECT * FROM test_1 WHERE cola < 500
//
// Should return 500 (number of output rows)


fun Lt500(vpi: *VectorProjectionIterator) -> int32 {
  var param: Integer = @intToSql(500)
  var cola: Integer
  if (@vpiIsFiltered(vpi)) {
    for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  } else {
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      cola = @vpiGetInt(vpi, 0)
      @vpiMatch(vpi, cola < param)
    }
  }
  @vpiResetFiltered(vpi)
  return 0
}

fun Lt500_Vec(vpi: *VectorProjectionIterator) -> int32 {
  return @filterLt(vpi, 0, 4, 500)
}

fun count(vpi: *VectorProjectionIterator) -> int32 {
  var ret = 0
  if (@vpiIsFiltered(vpi)) {
    for (; @vpiHasNextFiltered(vpi); @vpiAdvanceFiltered(vpi)) {
      ret = ret + 1
    }
  } else {
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      ret = ret + 1
    }
  }
  @vpiResetFiltered(vpi)
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
    var vpi = @tableIterGetVPI(&tvi)
    @filterManagerRunFilters(&filter, vpi)
    ret = ret + count(vpi)
  }

  @filterManagerFree(&filter)
  @tableIterClose(&tvi)
  return ret
}
