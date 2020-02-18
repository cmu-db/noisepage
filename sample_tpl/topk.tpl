struct output_struct {
  col1: Integer
}



fun pipeline_1(execCtx: *ExecutionContext) -> nil {
  var inttopK: IntegerTopKAggregate
  var address = &inttopK
  @integertopkaggInit(address, 100)

  var tvi: TableVectorIterator
  var col_oids : [2]uint32
  col_oids[0] = 1
  col_oids[1] = 2
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var cola = @pciGetInt(vec, 0)
        @integertopkaggAdvance(address,&cola)
    }
  }

  for(@integertopkaggHasResult(address)) {
    var out : *output_struct
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.col1 = @integertopkaggResult(address)
    @outputFinalize(execCtx)
  }

  @tableIterClose(&tvi)
}


fun main(execCtx: *ExecutionContext) -> int32 {
  // Run pipeline 1
  pipeline_1(execCtx)

  return 0
}
