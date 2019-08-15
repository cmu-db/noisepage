// Perform:
//
// SELECT col_b, count(col_a) FROM test_1 GROUP BY col_b
//
// Should output 10 (number of distinct col_b)

struct State {
  table: AggregationHashTable
  count: int32
}

struct Agg {
  key: Integer
  count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
  state.count = 0
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, pci: *ProjectedColumnsIterator) -> bool {
  var key = @pciGetInt(pci, 1)
  return @sqlToBool(key == agg.key)
}

fun constructAgg(agg: *Agg, pci: *ProjectedColumnsIterator) -> nil {
  agg.key = @pciGetInt(pci, 1)
  @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, pci: *ProjectedColumnsIterator) -> nil {
  var input = @pciGetInt(pci, 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var ht = &state.table
  var tvi: TableVectorIterator
  var col_oids : [2]uint32
  col_oids[0] = 1
  col_oids[1] = 2
  @tableIterInitBind(&tvi, execCtx, "test_1", col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var hash_val = @hash(@pciGetInt(vec, 1))
      var agg = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
      if (agg == nil) {
        agg = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
        constructAgg(agg, vec)
      } else {
        updateAgg(agg, vec)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun pipeline_2(execCtx: *ExecutionContext, state: *State) -> nil {
  var agg_ht_iter: AggregationHashTableIterator
  var iter = &agg_ht_iter

  for (@aggHTIterInit(iter, &state.table); @aggHTIterHasNext(iter); @aggHTIterNext(iter)) {
    var agg = @ptrCast(*Agg, @aggHTIterGetRow(iter))
    state.count = state.count + 1
  }
  @aggHTIterClose(iter)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State
  state.count = 0

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(execCtx, &state)

  // Run pipeline 2
  pipeline_2(execCtx, &state)

  var ret = state.count

  // Cleanup
  tearDownState(&state)

  return ret
}
