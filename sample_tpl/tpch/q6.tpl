struct outputStruct {
  out: Real
}

struct State {
  tvi: TableVectorIterator
  sum: RealSumAggregate
}

// This should be the first function for binding to occur correctly
fun setupTables(execCtx: *ExecutionContext, state: *State) -> nil {
  var tvi: TableVectorIterator
  @tableIterConstructBind(&state.tvi, "lineitem", execCtx, "li")
  @tableIterAddColBind(&state.tvi, "li", "l_quantity")
  @tableIterAddColBind(&state.tvi, "li", "l_extendedprice")
  @tableIterAddColBind(&state.tvi, "li", "l_discount")
  @tableIterAddColBind(&state.tvi, "li", "l_shipdate")
  @tableIterPerformInitBind(&state.tvi, "li")
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggInit(&state.sum)
  setupTables(execCtx, state)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @tableIterClose(&state.tvi)
}


fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (hashing)
  var tvi = &state.tvi
  for (@tableIterAdvance(tvi)) {
    var pci = @tableIterGetPCI(tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      if (@pciGetBind(pci, "li", "l_quantity") > 24.0
          and @pciGetBind(pci, "li", "l_discount") > 0.04
          and @pciGetBind(pci, "li", "l_discount") < 0.06
          and @pciGetBind(pci, "li", "l_shipdate") >= @dateToSql(1994, 1, 1)
          and @pciGetBind(pci, "li", "l_shipdate") <= @dateToSql(1995, 1, 1)) {
        var input = @pciGetBind(pci, "li", "l_extendedprice") * @pciGetBind(pci, "li", "l_discount")
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
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  setUpState(execCtx, &state)
  execQuery(execCtx, &state)
  teardownState(execCtx, &state)
  return 37
}