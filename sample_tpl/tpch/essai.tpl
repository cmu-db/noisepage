// This is what the codegen looks like for now.
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
}


fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (Aggregating)
  var tvi: TableVectorIterator
  @tableIterConstructBind(&tvi, "test_ns", "lineitem", execCtx)
  @tableIterPerformInit(&tvi)
  for (; @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var agg_values : AggValues
      agg_values.l_returnflag = @pciGetVarlenNull(vec, 0)
      agg_values.l_linestatus = @pciGetVarlenNull(vec, 1)
      agg_values.sum_qty = @pciGetDoubleNull(vec, 5)
      agg_values.sum_base_price = @pciGetDoubleNull(vec, 6)
      agg_values.sum_disc_price = @pciGetDoubleNull(vec, 6) * @pciGetDoubleNull(vec, 7)
      agg_values.sum_charge = @pciGetDoubleNull(vec, 6) * @pciGetDoubleNull(vec, 7) * (@floatToSql(1.0) - @pciGetDoubleNull(vec, 8))
      agg_values.avg_qty = @pciGetDoubleNull(vec, 5)
      agg_values.avg_price = @pciGetDoubleNull(vec, 6)
      agg_values.avg_disc = @pciGetDoubleNull(vec, 7)
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
  @tableIterClose(&tvi)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    pipeline1(execCtx, &state)
    return state.count
}