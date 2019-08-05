fun test() -> nil {}

fun main() -> int32 {
  test()
  return 0
}


// The code is written to (almost) match the way it will be codegened, so it may be overly verbose.
// But technically, some of these structs are redundant (like AggValues or SorterRow)

struct OutputStruct {
  n_name : StringVal
  revenue: Real
}

struct State {
  r_tvi : TableVectorIterator
  n_tvi : TableVectorIterator
  c_tvi : TableVectorIterator
  o_tvi : TableVectorIterator
  l_tvi : TableVectorIterator
  s_tvi : TableVectorIterator
  join_table1: JoinHashTable
  join_table2: JoinHashTable
  join_table3: JoinHashTable
  join_table4: JoinHashTable
  join_table5: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int64 // For debugging
}

fun setupTables(execCtx: *ExecutionContext, state: *State) -> nil {
  @tableIterConstructBind(&state.r_tvi, "region", execCtx, "r")
  @tableIterAddColBind(&state.r_tvi, "r", "r_regionkey")
  @tableIterAddColBind(&state.r_tvi, "r", "r_name")
  @tableIterPerformInitBind(&state.r_tvi, "r")

  @tableIterConstructBind(&state.n_tvi, "nation", execCtx, "n")
  @tableIterAddColBind(&state.n_tvi, "n", "n_nationkey")
  @tableIterAddColBind(&state.n_tvi, "n", "n_regionkey")
  @tableIterAddColBind(&state.n_tvi, "n", "n_name")
  @tableIterPerformInitBind(&state.n_tvi, "n")

  @tableIterConstructBind(&state.c_tvi, "customer", execCtx, "c")
  @tableIterAddColBind(&state.c_tvi, "c", "c_custkey")
  @tableIterAddColBind(&state.c_tvi, "c", "c_nationkey")
  @tableIterPerformInitBind(&state.c_tvi, "c")

  @tableIterConstructBind(&state.o_tvi, "orders", execCtx, "o")
  @tableIterAddColBind(&state.o_tvi, "o", "o_orderkey")
  @tableIterAddColBind(&state.o_tvi, "o", "o_custkey")
  @tableIterAddColBind(&state.o_tvi, "o", "o_orderdate")
  @tableIterPerformInitBind(&state.o_tvi, "o")

  @tableIterConstructBind(&state.l_tvi, "lineitem", execCtx, "l")
  @tableIterAddColBind(&state.l_tvi, "l", "l_orderkey")
  @tableIterAddColBind(&state.l_tvi, "l", "l_suppkey")
  @tableIterAddColBind(&state.l_tvi, "l", "l_extendedprice")
  @tableIterAddColBind(&state.l_tvi, "l", "l_discount")
  @tableIterPerformInitBind(&state.l_tvi, "l")

  @tableIterConstructBind(&state.s_tvi, "supply", execCtx, "s")
  @tableIterAddColBind(&state.s_tvi, "s", "s_suppkey")
  @tableIterAddColBind(&state.s_tvi, "s", "s_nationkey")
  @tableIterPerformInitBind(&state.s_tvi, "s")
}

struct JoinRow1 {
  r_regionkey : Integer
}

struct JoinRow2 {
  n_name : StringVal
  n_nationkey : Integer
}

struct JoinRow3 {
  n_name : StringVal
  n_nationkey : Integer
  c_custkey : Integer
}

struct JoinRow4 {
  n_name : StringVal
  n_nationkey : Integer
  o_orderkey : Integer
}

struct JoinProbe5 {
  lineitem_row : *VectorProjectionIterator
  join_row4 : *JoinRow4
}

struct JoinRow5 {
  s_suppkey : Integer
  s_nationkey : Integer
}


// Aggregate payload
struct AggPayload {
  n_name: StringVal
  revenue : RealSumAggregate
}

// Input of aggregate
struct AggValues {
  n_name: StringVal
  revenue : Real
}

// Input and Output of sorter
struct SorterRow {
  n_name: StringVal
  revenue : Real
}

fun checkAggKeyFn(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.n_name != row.n_name) {
    return false
  }
  return true
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *ProjectedColumnsIterator) -> bool {
  if (@pciGetBind(probe, "n", "n_regionkey") != @pciGetBind(build, "r", "r_regionkey")) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow2) -> bool {
  if (@pciGetBind(probe, "c", "c_nationkey") != build.n_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow3) -> bool {
  if (@pciGetBind(probe, "o", "o_custkey") != build.c_custkey) {
    return false
  }
  return true
}

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *ProjectedColumnsIterator, build: *JoinRow4) -> bool {
  if (@pciGetBind(probe, "l", "l_orderkey") != build.o_orderkey) {
    return false
  }
  return true
}

fun checkJoinKey5(execCtx: *ExecutionContext, probe: *JoinProbe5, build: *JoinRow5) -> bool {
  if (probe.join_row4.n_nationkey != @pciGetBind(build, "s", "s_nationkey")) {
    return false
  }
  if (@pciGetBind(probe.lineitem_row, "l", "l_suppkey") != @pciGetBind(build, "s", "s_suppkey")) {
    return false
  }
  return true
}

fun checkAggKey(payload: *AggPayload, values: *AggValues) -> bool {
  if (payload.n_name != values.n_name) {
    return false
  }
  return true
}

fun sorterCompare(lhs: *SorterRow, rhs: *SorterRow) -> int32 {
  if (lhs.revenue < rhs.revenue) {
    return -1
  }
  if (lhs.revenue > rhs.revenue) {
    return 1
  }
  return 0
}


fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  // Init tables
  setupTables(execCtx, state)
  // Initialize hash tables
  @joinHTInit(&state.join_table1, @execCtxGetMem(execCtx), @sizeOf(RegionRow))
  @joinHTInit(&state.join_table2, @execCtxGetMem(execCtx), @sizeOf(JoinRow1))
  @joinHTInit(&state.join_table3, @execCtxGetMem(execCtx), @sizeOf(JoinRow2))
  @joinHTInit(&state.join_table4, @execCtxGetMem(execCtx), @sizeOf(JoinRow3))
  @joinHTInit(&state.join_table5, @execCtxGetMem(execCtx), @sizeOf(SupplyRow))

  // Initialize aggregate
  @aggHTInit(&state.agg_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))

  // Initialize Sorter
  @sorterInit(&state.sorter, @execCtxGetMem(execCtx), sorterCompare, @sizeOf(SorterRow))
  state.count = 0
}

fun teardownState(execCtx: *ExecutionContext, state: *State) -> nil {
  @tableIterClose(@state.r_tvi)
  @tableIterClose(@state.n_tvi)
  @tableIterClose(@state.c_tvi)
  @tableIterClose(@state.o_tvi)
  @tableIterClose(@state.l_tvi)
  @tableIterClose(@state.s_tvi)
  @joinHTFree(&state.join_table1)
  @joinHTFree(&state.join_table2)
  @joinHTFree(&state.join_table3)
  @joinHTFree(&state.join_table4)
  @joinHTFree(&state.join_table5)
  @aggHTFree(&state.agg_table)
  @sorterFree(&state.sorter)
}

// Scan Region table and build HT1
fun pipeline1(execCtx: *ExecutionContext, state: *State) -> nil {
  var r_tvi = &state.r_tvi
  for (@tableIterAdvance(r_tvi)) {
    vec = @tableIterGetPCI(r_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(build, "r", "r_name") < @stringToSql("ASIA")) {
        // Step 2: Insert into HT1
        var hash_val = @hash(@pciGetBind(build, "r", "r_regionkey"))
        var build_row1 = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table1, hash_val))
        build_row1.r_regionkey = @pciGetBind(build, "r", "r_regionkey")
      }
    }
  }
  // Step 3: Build HT1
  @joinHTBuild(&state.join_table1)
}

// Scan Nation table, probe HT1, build HT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var n_tvi = &state.n_tvi
  for (@tableIterAdvance(n_tvi)) {
    vec = @tableIterGetPCI(n_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Probe HT1
      var hash_val = @hash(@pciGetBind(build, "n", "n_regionkey"))
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&hti, &state.join_table1, hash_val); @joinHTIterHasNext(&hti, checkJoinKey1, execCtx, vec);) {
        var build_row1 = @ptrCast(*JoinRow1, @joinHTIterGetRow(&hti))

        // Step 3: Insert into join table 2
        var hash_val2 = @hash(@pciGetBind(build, "n", "n_nationkey"))
        var build_row2 = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table2, hash_val2))
        build_row2.n_nationkey = @pciGetBind(build, "n", "n_nationkey")
        build_row2.n_name = @pciGetBind(build, "n", "n_name")
      }
    }
  }
  // Step 4: Build HT2
  @joinHTBuild(&state.join_table2)
}

// Scan Customer table, probe HT2, build HT3
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
  var c_tvi = &state.c_tvi
  for (@tableIterAdvance(c_tvi)) {
    vec = @tableIterGetPCI(c_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Probe HT2
      var hash_val = @hash(@pciGetBind(build, "c", "c_nationkey"))
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&hti, &state.join_table2, hash_val); @joinHTIterHasNext(&hti, checkJoinKey2, execCtx, vec);) {
        var join_row2 = @ptrCast(*JoinRow2, @joinHTIterGetRow(&hti))

        // Step 3: Insert into join table 3
        var hash_val3 = @hash(@pciGetBind(build, "c", "c_custkey"))
        var build_row3 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table3, hash_val3))
        build_row3.n_nationkey = join_row2.n_nationkey
        build_row3.n_name = join_row2.n_name
        build_row3.c_custkey = @pciGetBind(build, "c", "c_custkey")
      }
    }
  }
  // Step 4: Build HT3
  @joinHTBuild(&state.join_table3)
}

// Scan Orders table, probe HT3, build HT4
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var o_tvi = &state.o_tvi
  for (@tableIterAdvance(o_tvi)) {
    vec = @tableIterGetPCI(o_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      if (@pciGetBind(build, "o", "o_orderdate") >= @dateToSql(1994, 1, 1)
          and @pciGetBind(build, "o", "o_orderdate") < @dateToSql(1995, 1, 1)) {
        // Step 2: Probe HT3
        var hash_val = @hash(@pciGetBind(build, "o", "o_custkey"))
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table3, hash_val); @joinHTIterHasNext(&hti, checkJoinKey3, execCtx, vec);) {
          var join_row3 = @ptrCast(*JoinRow3, @joinHTIterGetRow(&hti))
          // Step 3: Insert into join table 4
          var hash_val4 = @hash(@pciGetBind(build, "o", "o_orderkey"))
          var build_row4 = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table4, hash_val))
          build_row4.n_nationkey = join_row3.n_nationkey
          build_row4.n_name = join_row3.n_name
          build_row4.o_orderkey = @pciGetBind(build, "o", "o_orderkey")
        }
      }
    }
  }
  // Step 4: Build HT4
  @joinHTBuild(&state.join_table4)
}

// Scan Supply, build join HT5
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
  var s_tvi = &state.s_tvi
  for (@tableIterAdvance(s_tvi)) {
    vec = @tableIterGetPCI(s_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Insert into HT5
      var hash_val = @hash(@pciGetBind(build, "s", "s_suppkey"), @pciGetBind(build, "s", "s_nationkey"))
      var build_row = @ptrCast(*SupplyRow, @joinHTInsert(&state.join_table5, hash_val))
      build_row.s_suppkey = @pciGetBind(build, "s", "s_suppkey")
      build_row.s_nationkey = @pciGetBind(build, "s", "s_nationkey")
    }
  }
  // Build
  @joinHTBuild(&state.join_table5)
}


// Scan Lineitem, probe HT4, probe HT5, build agg HT
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
  var l_tvi = &state.l_tvi
  for (@tableIterAdvance(l_tvi)) {
    vec = @tableIterGetPCI(l_tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      // Step 2: Probe HT4
      var hash_val = @hash(@pciGetBind(build, "l", "l_orderkey"))
      var hti: JoinHashTableIterator
      for (@joinHTIterInit(&hti, &state.join_table4, hash_val); @joinHTIterHasNext(&hti, checkJoinKey4, execCtx, vec);) {
        var join_row4 = @ptrCast(*JoinRow4, @joinHTIterGetRow(&hti))
        // Step 3: Probe HT5
        var hash_val5 = @hash(@pciGetBind(build, "l", "l_orderkey"), join_row4.n_nationkey)
        var join_probe5 : JoinProbe5 // Materialize the right pipeline
        join_probe5.lineitem_row = vec
        join_probe5.join_row4 = &join_row4
        var hti5: JoinHashTableIterator
        for (@joinHTIterInit(&hti5, &state.join_table5, hash_val5); @joinHTIterHasNext(&hti5, checkJoinKey5, execCtx, &join_probe5);) {
          var join_row5 = @ptrCast(*JoinRow5, @joinHTIterGetRow(&hti5))

          // Step 4: Build Agg HT
          var agg_input : AggValues // Materialize
          agg_input.n_name = join_row4.n_name
          agg_input.revenue = @pciGetBind(build, "l", "l_extendedprice") * (1 - @pciGetBind(build, "l", "l_discount"))
          var agg_hash_val = @hash(join_row3.n_name)
          var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_table, agg_hash_val, checkAggKey, &agg_input))
          if (agg_payload == nil) {
            agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
            agg_payload.n_name = agg_input.n_name
            @aggInit(&agg_payload.revenue)
          }
          @aggAdvance(&agg_payload.revenue, &agg_input.revenue)
        }
      }
    }
  }
}

// Scan Agg HT table, sort
fun pipeline7(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var agg_iter = &agg_ht_iter
  // Step 1: Iterate through Agg Hash Table
  for (@aggHTIterInit(agg_iter, &state.agg_table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {
    var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
    // Step 2: Build Sorter
    var sorter_row = @ptrCast(*SorterRow, @sorterInsert(&state.sorter))
    sorter_row.n_name = agg_payload.n_name
    sorter_row.revenue = @aggResult(&agg_payload.revenue)
  }
  @sorterSort(&state.sorter)
  @aggHTIterClose(agg_iter)
}

// Scan sorter, output
fun pipeline8(execCtx: *ExecutionContext, state: *State) -> nil {
  var sort_iter: SorterIterator
  var out: *OutputStruct
  for (@sorterIterInit(&sort_iter, &state.sorter); @sorterIterHasNext(&sort_iter); @sorterIterNext(&sort_iter)) {
    out = @ptrCast(*OutputStruct, @outputAlloc(execCtx))
    var sorter_row = @ptrCast(*SorterRow, @sorterIterGetRow(&sort_iter))
    out.n_name = sorter_row.n_name
    out.revenue = sorter_row.revenue
    @outputAdvance(execCtx)
  }
  @outputFinalize(execCtx)
  @sorterIterClose(&sort_iter)
}

fun execQuery(execCtx: *ExecutionContext, state: *State) -> nil {
  pipeline1(execCtx, state)
  //pipeline2(execCtx, state)
  //pipeline3(execCtx, state)
  //pipeline4(execCtx, state)
  //pipeline5(execCtx, state)
  //pipeline6(execCtx, state)
  //pipeline7(execCtx, state)
  //pipeline8(execCtx, state)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}