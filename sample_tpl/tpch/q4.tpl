struct OutputStruct {
  o_orderpriority: StringVal
  order_count: Integer
}


struct JoinBuildRow {
  o_orderkey: Integer
  o_orderpriority: StringVal
}

// Input & Output of the aggregator
struct AggRow {
  o_orderpriority: StringVal
  order_count: CountStarAggregate
}

// Input & output of the sorter
struct SorterRow {
  o_orderpriority: StringVal
  order_count: Integer
}

struct State {
  l_tvi: TableVectorIterator
  o_tvi: TableVectorIterator
  join_table: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int64
}

fun setupTables(execCtx: *ExecutionContext, state: *State) -> nil {
  @tableIterConstructBind(&state.l_tvi, "lineitem", execCtx, "l")
  @tableIterAddColBind(&state.l_tvi, "l", "l_orderkey")
  @tableIterAddColBind(&state.l_tvi, "l", "l_commitdate")
  @tableIterAddColBind(&state.l_tvi, "l", "l_receiptdate")
  @tableIterPerformInitBind(&state.l_tvi, "l")

  @tableIterConstructBind(&state.o_tvi, "orders", execCtx, "o")
  @tableIterAddColBind(&state.o_tvi, "o", "o_orderpriority")
  @tableIterAddColBind(&state.o_tvi, "o", "o_orderdate")
  @tableIterAddColBind(&state.o_tvi, "o", "o_orderkey")
  @tableIterPerformInitBind(&state.o_tvi, "o")
}

// Check that two join keys are equal
fun checkJoinKey(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build_row: *JoinBuildRow) -> bool {
  return @sqlToBool(@pciGetBind(probe, "l", "l_orderkey") == build_row.o_orderkey)
}

// Check that the aggregate key already exists
fun checkAggKey(agg: *AggRow, build_row: *JoinBuildRow) -> bool {
  return @sqlToBool(agg.o_orderpriority == build_row.o_orderpriority)
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
  setupTables(execCtx, state)
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggRow))
  @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(JoinBuildRow))
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
    @tableIterClose(&state.l_tvi)
    @tableIterClose(&state.o_tvi)
    @aggHTFree(&state.agg_table)
    @sorterFree(&state.sorter)
    @joinHTFree(&state.join_table)
}


// Pipeline 1 (Join Build)
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var o_tvi = &state.o_tvi
  for (@tableIterAdvance(o_tvi)) {
  state.count = state.count + 1
    var vec = @tableIterGetPCI(o_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(vec, "o", "o_orderdate") >= @dateToSql(1993, 07, 01)
          and @pciGetBind(vec, "o", "o_orderdate") < @dateToSql(1993, 10, 01)) {
        // Step 2: Insert into Hash Table
        var hash_val = @hash(@pciGetBind(vec, "o", "o_orderkey"))
        var build_row = @ptrCast(*JoinBuildRow, @joinHTInsert(&state.join_table, hash_val))
        build_row.o_orderkey = @pciGetBind(vec, "o", "o_orderkey")
        build_row.o_orderpriority = @pciGetBind(vec, "o", "o_orderpriority")
        state.count = state.count + 1
      }
    }
  }
  // Build table
  @joinHTBuild(&state.join_table)
}

// Pipeline 2 (Join Probe up to Agg)
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  var l_tvi = &state.l_tvi
  for (@tableIterAdvance(l_tvi)) {
    var vec = @tableIterGetPCI(l_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(vec, "l", "l_commitdate") < @pciGetBind(vec, "l", "l_receiptdate")) {
        // Step 2: Probe Join Hash Table
        var hash_val = @hash(@pciGetBind(vec, "l", "l_orderkey"))
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table, hash_val); @joinHTIterHasNext(&hti, checkJoinKey, execCtx, vec);) {
          var build_row = @ptrCast(*JoinBuildRow, @joinHTIterGetRow(&hti))
          // Step 3: Build Agg Hash Table
          var agg_hash_val = @hash(build_row.o_orderpriority)
          var agg = @ptrCast(*AggRow, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, build_row))
          if (agg == nil) {
            agg = @ptrCast(*AggRow, @aggHTInsert(&state.agg_table, agg_hash_val))
            agg.o_orderpriority = build_row.o_orderpriority
            @aggInit(&agg.order_count)
          }
          @aggAdvance(&agg.order_count, &build_row.o_orderpriority)
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
    sorter_row.o_orderpriority = agg.o_orderpriority
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