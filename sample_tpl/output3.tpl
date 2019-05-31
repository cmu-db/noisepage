// WORK IN PROGRESS: how the generated codegen code should look like

struct query_state_struct {
  col1: Integer
  col2: Integer
}

// SELECT colB, colC from test_1 WHERE colA < 500

fun query_init(qs : *query_state_struct) -> nil {
}

fun query_produce(qs : *query_state_struct) -> nil {
  for (id0 in test_1) {
    if (id0.colA < 500) {
      var out = @ptrCast(*query_state_struct, @outputAlloc())
      out.col1 = id0.colB
      out.col2 = id0.colC
      @outputAdvance()
    }
  }
}

fun query_teardown(qs : *query_state_struct) -> nil {
  @outputFinalize()
}


fun main() -> int {
  var query_state : query_state_struct
  query_init(&query_state)
  query_produce(&query_state)
  query_teardown(&query_state)
  return 0
}