// The code is written to (almost) match the way it will be codegened, so it may be overly verbose.
// But technically, some of these structs are redundant (like AggValues or SorterRow)

struct OutputStruct {
  n_name : Integer // TODO: Make this a string or Varlen
  revenue: Integer
}

struct State {
  join_table1: JoinHashTable
  join_table2: JoinHashTable
  join_table3: JoinHashTable
  join_table4: JoinHashTable
  join_table5: JoinHashTable
  agg_table: AggregationHashTable
  sorter: Sorter
  count: int64 // For debugging
}

// Row from region table
struct RegionRow {
  r_regionkey : Integer
  r_name : Integer
}

// Row from the nation table
struct NationRow {
  n_name : Integer // TODO: Make this a varlen
  n_nationkey : Integer
  n_regionkey : Integer
}

// Output of join(region, nation)
struct JoinRow1 {
  n_name : Integer // TODO: Make this a varlen
  n_nationkey : Integer
}

// Row of customer table
struct CustomerRow {
  c_custkey : Integer
  c_nationkey : Integer
}

// Output of join(join1, customer)
struct JoinRow2 {
  n_name : Integer // TODO: Make this a varlen
  n_nationkey : Integer
  c_custkey : Integer
}

// Row of orders table
struct OrdersRow {
  o_orderkey : Integer
  o_custkey : Integer
  o_orderdate : Integer // TODO: Make this a date
}

// Output of join(join2, orders)
struct JoinRow3 {
  n_name : Integer // TODO: Make this a varlen
  n_nationkey : Integer
  o_orderkey : Integer
}

// Row of lineitem table
struct LineitemRow {
  l_orderkey : Integer
  l_suppkey : Integer
  l_extendedprice : Integer
  l_discount : Integer
}

// Output of join(join3, lineitem)
struct JoinRow4 {
  n_name : Integer // TODO: Make this a varlen
  n_nationkey: Integer
  l_suppkey : Integer
  l_extendedprice : Integer
  l_discount : Integer
}


// Row of supply table
struct SupplyRow {
  s_suppkey: Integer
  s_nationkey: Integer
}

// Aggregate payload
struct AggPayload {
  n_name: Integer // TODO: Make this a varlen
  revenue : IntegerSumAggregate
}

// Input and Output of aggregate
struct AggValues {
  n_name: Integer // TODO: Make this a varlen
  revenue : Integer
}

// Input and Output of sorter
struct SorterRow {
  n_name: Integer // TODO: Make this a varlen
  revenue : Integer
}

fun checkAggKeyFn(payload: *AggPayload, row: *AggValues) -> bool {
  if (payload.n_name != row.n_name) {
    return false
  }
  return true
}

fun checkJoinKey1(execCtx: *ExecutionContext, probe: *NationRow, build: *RegionRow) -> bool {
  if (probe.n_regionkey != build.r_regionkey) {
    return false
  }
  return true
}

fun checkJoinKey2(execCtx: *ExecutionContext, probe: *CustomerRow, build: *JoinRow1) -> bool {
  if (probe.c_nationkey != build.n_nationkey) {
    return false
  }
  return true
}

fun checkJoinKey3(execCtx: *ExecutionContext, probe: *OrdersRow, build: *JoinRow2) -> bool {
  if (probe.o_custkey != build.c_custkey) {
    return false
  }
  return true
}

fun checkJoinKey4(execCtx: *ExecutionContext, probe: *LineitemRow, build: *JoinRow3) -> bool {
  if (probe.l_orderkey != build.o_orderkey) {
    return false
  }
  return true
}

fun checkJoinKey5(execCtx: *ExecutionContext, probe: *JoinRow4, build: *SupplyRow) -> bool {
  if (probe.n_nationkey != build.s_nationkey) {
    return false
  }
  if (probe.l_suppkey != build.s_suppkey) {
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
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var region_row: RegionRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      region_row.r_regionkey = @pciGetInt(vec, 1)
      region_row.r_name = @pciGetInt(vec, 0)
      // TODO: Replace check with varlen comparison to 'ASIA'
      if (region_row.r_name < 100) {
        // Step 2: Insert into HT1
        var hash_val = @hash(region_row.r_regionkey)
        var build_row = @ptrCast(*RegionRow, @joinHTInsert(&state.join_table1, hash_val))
        build_row.r_regionkey = region_row.r_regionkey
      }
    }
  }
  // Step 3: Build HT1
  @joinHTBuild(&state.join_table1)
  @tableIterClose(&tvi)
}

// Scan Nation table, probe HT1, build HT2
fun pipeline2(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var nation_row: NationRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      nation_row.n_name = @pciGetInt(vec, 1)
      nation_row.n_nationkey = @pciGetInt(vec, 1)
      nation_row.n_regionkey = @pciGetInt(vec, 1)
      if (@pciGetInt(vec, 0) < 10) {
        // Step 2: Probe HT1
        var hash_val = @hash(nation_row.n_regionkey)
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table1, hash_val); @joinHTIterHasNext(&hti, checkJoinKey1, execCtx, &nation_row);) {
          var region_row = @ptrCast(*RegionRow, @joinHTIterGetRow(&hti))

          // Step 3: Insert into join table 2
          var hash_val2 = @hash(nation_row.n_nationkey)
          var build_row = @ptrCast(*JoinRow1, @joinHTInsert(&state.join_table2, hash_val))
          build_row.n_nationkey = nation_row.n_nationkey
          build_row.n_name = nation_row.n_name
        }
      }
    }
  }
  // Step 4: Build HT2
  @joinHTBuild(&state.join_table2)
  @tableIterClose(&tvi)
}

// Scan Customer table, probe HT2, build HT3
fun pipeline3(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var customer_row: CustomerRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      customer_row.c_custkey = @pciGetInt(vec, 1)
      customer_row.c_nationkey = @pciGetInt(vec, 1)
      if (@pciGetInt(vec, 0) < 10) {
        // Step 2: Probe HT2
        var hash_val = @hash(customer_row.c_nationkey)
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table2, hash_val); @joinHTIterHasNext(&hti, checkJoinKey2, execCtx, &customer_row);) {
          var join_row1 = @ptrCast(*JoinRow1, @joinHTIterGetRow(&hti))

          // Step 3: Insert into join table 3
          var hash_val3 = @hash(customer_row.c_custkey)
          var build_row = @ptrCast(*JoinRow2, @joinHTInsert(&state.join_table3, hash_val))
          build_row.n_nationkey = join_row1.n_nationkey
          build_row.n_name = join_row1.n_name
          build_row.c_custkey = customer_row.c_custkey
        }
      }
    }
  }
  // Step 4: Build HT3
  @joinHTBuild(&state.join_table3)
  @tableIterClose(&tvi)
}

// Scan Orders table, probe HT3, build HT4
fun pipeline4(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var orders_row: OrdersRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      orders_row.o_custkey = @pciGetInt(vec, 1)
      orders_row.o_orderkey = @pciGetInt(vec, 1)
      orders_row.o_orderdate = @pciGetInt(vec, 0)
      // TODO: Replace by a date check
      if (orders_row.o_orderdate < 10) {

        // Step 2: Probe HT3
        var hash_val = @hash(orders_row.o_custkey)
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table3, hash_val); @joinHTIterHasNext(&hti, checkJoinKey3, execCtx, &orders_row);) {
          var join_row2 = @ptrCast(*JoinRow2, @joinHTIterGetRow(&hti))
          // Step 3: Insert into join table 4
          var hash_val4 = @hash(orders_row.o_orderkey)
          var build_row = @ptrCast(*JoinRow3, @joinHTInsert(&state.join_table4, hash_val))
          build_row.n_nationkey = join_row2.n_nationkey
          build_row.n_name = join_row2.n_name
          build_row.o_orderkey = orders_row.o_orderkey
        }
      }
    }
  }
  // Step 4: Build HT4
  @joinHTBuild(&state.join_table4)
  @tableIterClose(&tvi)
}

// Scan Supply, build join HT5
fun pipeline5(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var supply_row: SupplyRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      supply_row.s_suppkey = @pciGetInt(vec, 1)
      supply_row.s_nationkey = @pciGetInt(vec, 1)
      if (@pciGetInt(vec, 0) < 10) {
        // Step 2: Insert into HT5
        var hash_val = @hash(supply_row.s_suppkey)
        var build_row = @ptrCast(*SupplyRow, @joinHTInsert(&state.join_table5, hash_val))
        build_row.s_suppkey = supply_row.s_suppkey
        build_row.s_nationkey = supply_row.s_nationkey
      }
    }
  }
  // Build
  @joinHTBuild(&state.join_table5)
  @tableIterClose(&tvi)
}


// Scan Lineitem, probe HT4, probe HT5, build agg HT
fun pipeline6(execCtx: *ExecutionContext, state: *State) -> nil {
  // Step 1: Sequential Scan
  var tvi: TableVectorIterator
  var vec: *ProjectedColumnsIterator
  var lineitem_row: LineitemRow
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      lineitem_row.l_orderkey = @pciGetInt(vec, 1)
      lineitem_row.l_suppkey = @pciGetInt(vec, 1)
      lineitem_row.l_extendedprice = @pciGetInt(vec, 1)
      lineitem_row.l_discount = @pciGetInt(vec, 1)
      if (@pciGetInt(vec, 0) < 100) {

        // Step 2: Probe HT4
        var hash_val = @hash(lineitem_row.l_orderkey)
        var hti: JoinHashTableIterator
        for (@joinHTIterInit(&hti, &state.join_table4, hash_val); @joinHTIterHasNext(&hti, checkJoinKey4, execCtx, &lineitem_row);) {
          var join_row3 = @ptrCast(*JoinRow3, @joinHTIterGetRow(&hti))
          if (join_row3.n_nationkey < 100) {
            // Step 3: Probe HT5
            var hash_val2 = @hash(lineitem_row.l_suppkey)
            var join_row4 : JoinRow4 // Materialize the right pipeline
            join_row4.n_name = join_row3.n_name
            join_row4.n_nationkey = join_row3.n_nationkey
            join_row4.l_suppkey = lineitem_row.l_suppkey
            join_row4.l_extendedprice = lineitem_row.l_extendedprice
            join_row4.l_discount = lineitem_row.l_discount
            var hti2: JoinHashTableIterator
            @joinHTIterInit(&hti2, &state.join_table5, hash_val2)
            for (@joinHTIterInit(&hti2, &state.join_table5, hash_val2); @joinHTIterHasNext(&hti2, checkJoinKey5, execCtx, &join_row4);) {
              var supply_row = @ptrCast(*SupplyRow, @joinHTIterGetRow(&hti2))

              // Step 4: Build Agg HT
              var agg_input : AggValues // Materialize
              agg_input.n_name = join_row4.n_name
              agg_input.revenue = @intToSql(1) // join_row4.l_extendedprice * (1 - join_row4.l_discount)
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
  }
  @tableIterClose(&tvi)
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
  pipeline2(execCtx, state)
  pipeline3(execCtx, state)
  pipeline4(execCtx, state)
  pipeline5(execCtx, state)
  pipeline6(execCtx, state)
  pipeline7(execCtx, state)
  pipeline8(execCtx, state)
}


fun main(execCtx: *ExecutionContext) -> int32 {
    var state: State
    setUpState(execCtx, &state)
    execQuery(execCtx, &state)
    teardownState(execCtx, &state)
    return state.count
}