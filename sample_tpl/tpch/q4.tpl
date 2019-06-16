struct OutputStruct {
  o_orderpriority: Integer
  order_count: Integer
}

// Orders Seq Scan output and Left Join input
struct OrdersRow {
  o_orderpriority: Integer
  o_orderkey: Integer
  o_orderdate: Integer
}

// Lineitem Seq Scan output and Right Join input
struct LineItemRow {
  l_receiptdate: Integer
  l_commitdate: Integer
  l_orderkey: Integer
}

// Key of the aggregate
struct AggKey {
  o_orderpriority: Integer
}

// Input & Output of the aggregator
struct AggRow {
  key: AggKey
  order_count: CountStarAggregate
}

// Input & output of the sorter
struct SorterRow {
  o_orderpriority: Integer
  order_count: Integer
}

struct State {
  join_table: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int32
}

// Check that two join keys are equal
fun checkJoinKey(execCtx: *ExecutionContext, probe: *LineItemRow, tuple: *OrdersRow) -> bool {
  return @sqlToBool(probe.l_orderkey == tuple.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggRow, orders_row: *OrdersRow) -> bool {
  return @sqlToBool(agg.key.o_orderpriority == orders_row.o_orderpriority)
}

// Initialize aggregator for a new key
fun constructAgg(agg: *AggRow, orders_row: *OrdersRow) -> nil {
  agg.key.o_orderpriority = orders_row.o_orderpriority
  @aggInit(&agg.order_count)

}

fun updateAgg(agg: *AggRow, orders_row: *OrdersRow) -> nil {
  @aggAdvance(&agg.order_count, &orders_row.o_orderpriority)
}

// Sorter comparison function
fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.o_orderpriority < rhs.o_orderpriority) {
    return -1
  } else if (lhs.o_orderpriority > lhs.o_orderpriority) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggRow))
  @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(OrdersRow))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
    @joinHTFree(&state.join_table)
}


// Pipeline 1 (Join Build)
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var tvi: TableVectorIterator
  var orders_row: OrdersRow
  // Step 1: Sequential Scan
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // TODO: Replace with right condition and offsets
      orders_row.o_orderpriority = @pciGetInt(vec, 1)
      orders_row.o_orderdate = @pciGetInt(vec, 1)
      orders_row.o_orderkey = @pciGetInt(vec, 0)
      if (orders_row.o_orderkey < 500) {
        // Step 2: Insert into Hash Table
        var hash_val = @hash(orders_row.o_orderkey)
        var build_elem : *OrdersRow = @ptrCast(*OrdersRow, @joinHTInsert(&state.join_table, hash_val))
        build_elem.o_orderpriority = orders_row.o_orderpriority
        build_elem.o_orderdate = orders_row.o_orderdate
        build_elem.o_orderkey = orders_row.o_orderkey
      }
    }
  }
  @tableIterClose(&tvi)
  // Build table
  @joinHTBuild(&state.join_table)
}

// Pipeline 2 (Join Probe up to Agg)
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  var lineitem_row: LineItemRow
  var tvi : TableVectorIterator
  var orders_row: *OrdersRow
  // Step 1: Sequential Scan
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      lineitem_row.l_orderkey = @pciGetInt(vec, 0)
      lineitem_row.l_commitdate = @pciGetInt(vec, 1)
      lineitem_row.l_receiptdate = @pciGetInt(vec, 1)
      if (lineitem_row.l_orderkey < 500) {
        // Step 2: Probe Join Hash Table
        var hash_val = @hash(lineitem_row.l_orderkey)
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table, hash_val); @joinHTIterHasNext(&hti, checkJoinKey, execCtx, &lineitem_row);) {
          orders_row = @ptrCast(*OrdersRow, @joinHTIterGetRow(&hti))
          // Step 3: Build Agg Hash Table
          var agg_hash_val = @hash(orders_row.o_orderpriority)
          var agg = @ptrCast(*AggRow, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, orders_row))
          if (agg == nil) {
            agg = @ptrCast(*AggRow, @aggHTInsert(&state.agg_table, agg_hash_val))
            constructAgg(agg, orders_row)
          }
          updateAgg(agg, orders_row)
        }
        @joinHTIterClose(&hti)
      }
    }
  }
}

// Pipeline 3 (Sort)
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var agg_iter = &agg_ht_iter
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
    var agg = @ptrCast(*AggRow, @aggHTIterGetRow(agg_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
    sorter_row.o_orderpriority = agg.key.o_orderpriority
    sorter_row.order_count = @aggResult(&agg.order_count)
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(agg_iter)
}

// Pipeline 4 (Output)
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  var out: *OutputStruct
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.o_orderpriority = sorter_row.o_orderpriority
    out.order_count = sorter_row.order_count
    @outputAdvance(execCtx)
  }
  @outputFinalize(execCtx)
  @sorterIterClose(&sort_iter)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  pipeline1(execCtx, state)
  pipeline2(execCtx, state)
  pipeline3(execCtx, state)
  pipeline4(execCtx, state)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}