struct outputStruct {
    l_returnflag : Integer
    l_linestatus : Integer
    sum_qty : Integer
    sum_base_price : Integer
    sum_disc_price : Integer
    sum_charge : Integer
    avg_qty : Integer
    avg_price : Integer
    avg_disc : Integer
    count_order : Integer
}

struct Row {
    l_returnflag : Integer
    l_linestatus : Integer
    l_quantity : Integer
    l_extendedprice : Integer
    l_discount : Integer
    l_tax : Integer
}

struct State {
    table: AggregationHashTable
    sorter: Sorter
    count : int32
}

struct AggKey {
    l_returnflag : Integer
    l_linestatus : Integer
}

struct Agg {
    key : AggKey
    sum_qty : IntegerSumAggregate
    sum_base_price : IntegerSumAggregate
    sum_disc_price : IntegerSumAggregate
    sum_charge : IntegerSumAggregate
    avg_qty : IntegerAvgAggregate
    avg_price : IntegerAvgAggregate
    avg_disc : IntegerAvgAggregate
    count_order : CountStarAggregate
}

struct SorterRow {
    l_returnflag : Integer
    l_linestatus : Integer
    sum_qty : Integer
    sum_base_price : Integer
    sum_disc_price : Integer
    sum_charge : Integer
    avg_qty : Integer
    avg_price : Integer
    avg_disc : Integer
    count_order : Integer
}

fun keyCheck(agg: *Agg, pci: *ProjectedColumnsIterator) -> bool {
  // TODO: Get the correct indices once lineitem is generated.
  var key : AggKey
  key.l_returnflag = @pciGetInt(pci, 1)
  key.l_linestatus = @pciGetInt(pci, 1)
  return @sqlToBool(key.l_returnflag == agg.key.l_returnflag) and @sqlToBool(key.l_linestatus == agg.key.l_linestatus)
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
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), compareFn, @sizeOf(SorterRow))
  state.count = 0
}

// Initialize all aggregates
fun constructAgg(agg: *Agg, pci: *ProjectedColumnsIterator) -> nil {
    // TODO: Get the correct indices.
    agg.key.l_returnflag = @pciGetInt(pci, 1)
    agg.key.l_linestatus = @pciGetInt(pci, 1)

    @aggInit(&agg.sum_qty)
    @aggInit(&agg.sum_base_price)
    @aggInit(&agg.sum_disc_price)
    @aggInit(&agg.sum_charge)
    @aggInit(&agg.avg_qty)
    @aggInit(&agg.avg_price)
    @aggInit(&agg.avg_disc)
    @aggInit(&agg.count_order)
}

// Update all aggregates
fun updateAgg(agg: *Agg, pci: *ProjectedColumnsIterator) -> nil {
    // Store row to avoid copying
    // TODO: Get the correct indices.
    var row : Row
    row.l_returnflag = @pciGetInt(pci, 1)
    row.l_linestatus = @pciGetInt(pci, 1)
    row.l_quantity = @pciGetInt(pci, 1)
    row.l_extendedprice = @pciGetInt(pci, 1)
    row.l_discount = @pciGetInt(pci, 1)
    row.l_tax = @pciGetInt(pci, 1)

    // sum_qty
    @aggAdvance(&agg.sum_qty, &row.l_quantity)

    // sum_base_price
    @aggAdvance(&agg.sum_base_price, &row.l_extendedprice)

    // sum_charge
    var sum_disc_price_input = @intToSql(0) //(row.l_extendedprice * (1 - row.l_discount))
    @aggAdvance(&agg.sum_disc_price, &sum_disc_price_input)

    // sum_charge_input
    var sum_charge_input = @intToSql(0) //(row.l_extendedprice * (1 - row.l_discount)) * (1 + row.l_tax)
    @aggAdvance(&agg.sum_charge, &sum_charge_input)

    // avg_qty
    @aggAdvance(&agg.avg_qty, &row.l_quantity)

    // avg_price
    @aggAdvance(&agg.avg_price, &row.l_extendedprice)

    // avg_disc
    @aggAdvance(&agg.avg_disc, &row.l_discount)

    // count_order
    @aggAdvance(&agg.count_order, &row.l_quantity)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
    var out : *outputStruct

    // Pipeline 1 (Aggregating)
    var ht = &state.table
    var tvi: TableVectorIterator
    for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetPCI(&tvi)
        for (; @pciHasNext(vec); @pciAdvance(vec)) {
            // TODO: Get Right columns
            var hash_val = @hash(@pciGetInt(vec, 1), @pciGetInt(vec, 1))
            var agg1 = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
            if (agg1 == nil) {
                agg1 = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
                constructAgg(agg1, vec)
                state.count = state.count + 1
            } else {
                updateAgg(agg1, vec)
            }
        }
    }
    @tableIterClose(&tvi)

    // Pipeline 2 (Sorting)
    var agg_ht_iter: AggregationHashTableIterator
    var agg_iter = &agg_ht_iter
    for (@aggHTIterInit(agg_iter, &state.table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
        var agg2 = @ptrCast(*Agg, @aggHTIterGetRow(agg_iter))
        var sorter_row2 = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
        sorter_row2.l_returnflag = agg2.key.l_returnflag
        sorter_row2.l_linestatus = agg2.key.l_linestatus
        sorter_row2.sum_qty = @aggResult(&agg2.sum_qty)
        sorter_row2.sum_base_price = @aggResult(&agg2.sum_base_price)
        sorter_row2.sum_disc_price = @aggResult(&agg2.sum_disc_price)
        sorter_row2.sum_charge = @aggResult(&agg2.sum_charge)
        sorter_row2.avg_qty = @aggResult(&agg2.avg_qty)
        sorter_row2.avg_price = @aggResult(&agg2.avg_price)
        sorter_row2.avg_disc = @aggResult(&agg2.avg_disc)
        sorter_row2.count_order = @aggResult(&agg2.count_order)
    }
    @sorterSort(&state.sorter)
    @aggHTIterClose(agg_iter)

    // Pipeline 3 (Output to upper layers)
    var sort_iter: SorterIterator
    for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
        out = @ptrCast(*outputStruct, @outputAlloc(execCtx))
        var sorter_row3 = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
        out.l_returnflag = sorter_row3.l_returnflag
        out.l_linestatus = sorter_row3.l_linestatus
        out.sum_qty = sorter_row3.sum_qty
        out.sum_base_price = sorter_row3.sum_base_price
        out.sum_disc_price = sorter_row3.sum_disc_price
        out.sum_charge = sorter_row3.sum_charge
        out.avg_qty = sorter_row3.avg_qty
        out.avg_price = sorter_row3.avg_price
        out.avg_disc = sorter_row3.avg_disc
        out.count_order = sorter_row3.count_order
        @outputAdvance(execCtx)
    }
    @sorterIterClose(&sort_iter)
    @outputFinalize(execCtx)
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.table)
    @sorterFree(&state.sorter)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}