struct State {
  table: AggregationHashTable
}

struct Agg {
  key: Integer
  count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
  @aggHTInit(&state.table, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
  @aggHTFree(&state.table)
}

fun keyCheck(pci: *ProjectedColumnsIterator, agg: *Agg) -> bool {
  var key = @pciGetInt(pci, 0)
  return @sqlToBool(key == agg.key)
}

fun constructAgg(pci: *ProjectedColumnsIterator, agg: *Agg) -> nil {
  @aggInit(&agg.count)
}

fun updateAgg(pci: *ProjectedColumnsIterator, agg: *Agg) -> nil {
  var input = @pciGetInt(pci, 0)
  @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State) -> nil {
  var ht: *AggregationHashTable = &state.table

  var tvi: TableVectorIterator
  for (@tableIterInit(&tvi, "test_1", execCtx); @tableIterAdvance(&tvi); ) {
    var vec = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(vec); @pciAdvance(vec)) {
      var hash_val = @hash(@pciGetInt(vec, 0))
      var agg = @ptrCast(*Agg, @aggHTLookup(ht, hash_val, keyCheck, vec))
      if (agg == nil) {
        agg = @ptrCast(*Agg, @aggHTInsert(ht, hash_val))
        constructAgg(vec, agg)
      } else {
        updateAgg(vec, agg)
      }
    }
  }
  @tableIterClose(&tvi)
}

fun main(execCtx: *ExecutionContext) -> int32 {
  var state: State

  // Initialize state
  setUpState(execCtx, &state)

  // Run pipeline 1
  pipeline_1(execCtx, &state)

  // Cleanup
  tearDownState(&state)

  return 0
}
