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
  tvi : TableVectorIterator
  agg_hash_table: AggregationHashTable
  sorter: Sorter
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

struct AggPayload {
  l_returnflag: StringVal
  l_linestatus: StringVal
  sum_qty : RealSumAggregate
  sum_base_price : RealSumAggregate
  sum_disc_price : RealSumAggregate
  sum_charge : RealSumAggregate
  avg_qty : RealAvgAggregate
  avg_price : RealAvgAggregate
  avg_disc : RealAvgAggregate
  count_order : CountAggregate
}

struct SorterRow {
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

// This should be the first function for binding to occur correctly
fun setupTables(execCtx: *ExecutionContext, state: *State) -> nil {
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

fun compareFn(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.l_returnflag < rhs.l_returnflag) {
    return -1
  }
  if (lhs.l_returnflag > rhs.l_returnflag) {
    return 1
  }
  if (lhs.l_linestatus > rhs.l_linestatus) {
    return -1
  }
  if (lhs.l_linestatus > rhs.l_linestatus) {
    return 1
  }
  return 0
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.agg_hash_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
  state.count = 0
  setupTables(execCtx, state)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTFree(&state.agg_hash_table)
  @sorterFree(&state.sorter)
  @tableIterClose(&state.tvi)
}

fun aggKeyCheck(agg_payload: *AggPayload, agg_values: *AggValues) -> bool {
  if (agg_payload.l_returnflag != agg_values.l_returnflag) {
    return false
  }
  if (agg_payload.l_linestatus != agg_values.l_linestatus) {
    return false
  }
  return true
}


fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 1 (Aggregating)
  var tvi = &state.tvi
  for (@tableIterAdvance(tvi)) {
    var vec = @tableIterGetPCI(tvi)
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
      var agg_hash_val = @hash(agg_values.l_returnflag, agg_values.l_linestatus)
      var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_hash_table, agg_hash_val, aggKeyCheck, &agg_values))
      if (agg_payload == nil) {
        agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_hash_table, agg_hash_val))
        agg_payload.l_returnflag = agg_values.l_returnflag
        agg_payload.l_linestatus = agg_values.l_linestatus

        @aggInit(&agg_payload.sum_qty)
        @aggInit(&agg_payload.sum_base_price)
        @aggInit(&agg_payload.sum_disc_price)
        @aggInit(&agg_payload.sum_charge)
        @aggInit(&agg_payload.avg_qty)
        @aggInit(&agg_payload.avg_price)
        @aggInit(&agg_payload.avg_disc)
        @aggInit(&agg_payload.count_order)
      }
      @aggAdvance(&agg_payload.sum_qty, &agg_values.sum_qty)
      @aggAdvance(&agg_payload.sum_base_price, &agg_values.sum_base_price)
      @aggAdvance(&agg_payload.sum_disc_price, &agg_values.sum_disc_price)
      @aggAdvance(&agg_payload.sum_charge, &agg_values.sum_charge)
      @aggAdvance(&agg_payload.avg_qty, &agg_values.avg_qty)
      @aggAdvance(&agg_payload.avg_price, &agg_values.avg_price)
      @aggAdvance(&agg_payload.avg_disc, &agg_values.avg_disc)
      @aggAdvance(&agg_payload.count_order, &agg_values.count_order)
    }
  }
}

fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  // Pipeline 2 (Sorting)
  var agg_iter: AggregationHashTableIterator
  for (@aggHTIterInit(&agg_iter, &state.agg_hash_table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(&agg_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
    sorter_row.l_returnflag = agg_payload.l_returnflag
    sorter_row.l_linestatus = agg_payload.l_linestatus
    sorter_row.sum_qty = @aggResult(&agg_payload.sum_qty)
    sorter_row.sum_base_price = @aggResult(&agg_payload.sum_base_price)
    sorter_row.sum_disc_price = @aggResult(&agg_payload.sum_disc_price)
    sorter_row.sum_charge = @aggResult(&agg_payload.sum_charge)
    sorter_row.avg_qty = @aggResult(&agg_payload.avg_qty)
    sorter_row.avg_price = @aggResult(&agg_payload.avg_price)
    sorter_row.avg_disc = @aggResult(&agg_payload.avg_disc)
    sorter_row.count_order = @aggResult(&agg_payload.count_order)
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(&agg_iter)
}

fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
    // Pipeline 3 (Output to upper layers)
    var out: *Output
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        out = @ptrCast(*Output, @outputAlloc(execCtx))
        var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.l_returnflag = sorter_row.l_returnflag
        out.l_linestatus = sorter_row.l_linestatus
        out.sum_qty = sorter_row.sum_qty
        out.sum_base_price = sorter_row.sum_base_price
        out.sum_disc_price = sorter_row.sum_disc_price
        out.sum_charge = sorter_row.sum_charge
        out.avg_qty = sorter_row.avg_qty
        out.avg_price = sorter_row.avg_price
        out.avg_disc = sorter_row.avg_disc
        out.count_order = sorter_row.count_order
        @outputAdvance(execCtx)
    }
    @sorterIterClose(&sort_iter)
    @outputFinalize(execCtx)
}


fun main(execCtx: *ExecutionContext) -> int {
    var state: State
    setUpState(execCtx, &state)
    pipeline1(execCtx, &state)
    pipeline2(execCtx, &state)
    pipeline3(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}