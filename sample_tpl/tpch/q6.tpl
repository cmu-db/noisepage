struct outputStruct {
  out: Real
}

struct State {
  sum: RealSumAggregate
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
}


fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (hashing)
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "lineitem", execCtx)
  @tableIterAddColBind(&tvi, "test_ns", "lineitem", "l_quantity")
  @tableIterAddColBind(&tvi, "test_ns", "lineitem", "l_extendedprice")
  @tableIterAddColBind(&tvi, "test_ns", "lineitem", "l_discount")
  @tableIterAddColBind(&tvi, "test_ns", "lineitem", "l_shipdate")
  @tableIterPerformInit(&tvi)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      // TODO: Figure out lineitem column order in storage layer
      if (@pciGetIntNull(pci, 0) > 24 and @pciGetDoubleNull(pci, 2) > 0.04 and @pciGetDoubleNull(pci, 2) < 0.06
          and @pciGetDateNull(pci, 3) >= @dateToSql(1994, 1, 1) and @pciGetDateNull(pci, 3) <= @dateToSql(1995, 1, 1)) {
        var input = @pciGetDoubleNull(pci, 1) * @pciGetDoubleNull(pci, 2)
        @aggAdvance(&state.sum, &input)
      }
    }
  }

  // Pipeline 2 (Output to upper layers)
  var out : *outputStruct
  out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
  out.out = @aggResult(&state.sum)
  @outputAdvance(execCtx)
  @outputFinalize(execCtx)
  @tableIterClose(&tvi)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
}


fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  setUpState(execCtx, &state)
  execQuery(execCtx, &state)
  teardownState(execCtx, &state)
  return 37
}