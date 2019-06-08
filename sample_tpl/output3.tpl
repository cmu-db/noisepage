struct AggPayload {
  key1: Integer
  key2: Integer
  count: CountStarAggregate
}

struct AggRow {
  key1: Integer
  key2: Integer
  count: Integer
}

struct TableRow {
  col1: Integer
  col2: Integer
}

fun keyCheck(payload: *AggPayload, row: *TableRow) -> bool {
  return @sqlToBool(payload.key1 == row.col1) and @sqlToBool(payload.key2 == row.col2)
}



fun main(execCtx: *ExecutionContext) -> int {
  return 0
}