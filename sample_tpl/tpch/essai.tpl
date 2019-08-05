// This is what the codegen is supposed to look like for now.
// It will likely change once I add vectorized operations.

struct Output {
  l_returnflag : StringVal
  l_linestatus : StringVal
  sum_qty : Real
  sum_base_price : Real
  sum_disc_price : Real
  sum_charge : Real
  avg_qty : Real
  avg_price : Real
  avg_disc : Real
  count_order : Integer
}

struct State {
  tvi : TableVectorIterator
  count : int64 // debug
}

struct AggValues {
  l_returnflag: StringVal
  l_linestatus: StringVal
  sum_qty : Real
  sum_base_price : Real
  sum_disc_price : Real
  sum_charge : Real
  avg_qty : Real
  avg_price : Real
  avg_disc : Real
  count_order : Integer
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  state.count = 0
  @tableIterConstructBind(&state.tvi, "lineitem", execCtx, "li")
  @tableIterAddColBind(&state.tvi, "li", "l_returnflag")
  @tableIterAddColBind(&state.tvi, "li", "l_linestatus")
  @tableIterAddColBind(&state.tvi, "li", "l_quantity")
  @tableIterAddColBind(&state.tvi, "li", "l_extendedprice")
  @tableIterAddColBind(&state.tvi, "li", "l_discount")
  @tableIterAddColBind(&state.tvi, "li", "l_tax")
  @tableIterAddColBind(&state.tvi, "li", "l_shipdate")
  @tableIterPerformInitBind(&state.tvi, "li")
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @tableIterClose(&state.tvi)
}


fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (Aggregating)
  var li_tvi = &state.tvi
  for (@tableIterAdvance(li_tvi)) {
    var vec = @tableIterGetPCI(li_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var agg_values : AggValues
      agg_values.l_returnflag = @pciGetBind(vec, "li", "l_returnflag")
      agg_values.l_linestatus = @pciGetBind(vec, "li", "l_linestatus")
      agg_values.sum_qty = @pciGetBind(vec, "li", "l_quantity")
      agg_values.sum_base_price = @pciGetBind(vec, "li", "l_extendedprice")
      agg_values.sum_disc_price = @pciGetBind(vec, "li", "l_extendedprice") * @pciGetBind(vec, "li", "l_discount")
      agg_values.sum_charge = @pciGetBind(vec, "li", "l_extendedprice") * @pciGetBind(vec, "li", "l_discount") * (@floatToSql(1.0) - @pciGetBind(vec, "li", "l_tax"))
      agg_values.avg_qty = @pciGetBind(vec, "li", "l_quantity")
      agg_values.avg_price = @pciGetBind(vec, "li", "l_extendedprice")
      agg_values.avg_disc = @pciGetBind(vec, "li", "l_discount")
      agg_values.count_order = @intToSql(1)
      var out = @ptrCast(*Output, @outputAlloc(execCtx))
      out.l_returnflag = agg_values.l_returnflag
      out.l_linestatus = agg_values.l_linestatus
      out.sum_qty = agg_values.sum_qty
      out.sum_base_price = agg_values.sum_base_price
      out.sum_disc_price = agg_values.sum_disc_price
      out.sum_charge = agg_values.sum_charge
      out.avg_qty = agg_values.avg_qty
      out.avg_price = agg_values.avg_price
      out.avg_disc = agg_values.avg_disc
      out.count_order = agg_values.count_order
      @outputAdvance(execCtx)
    }
  }
  @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int64 {
    var state: State
    setUpState(execCtx, &state)
    pipeline1(execCtx, &state)
    return state.count
}