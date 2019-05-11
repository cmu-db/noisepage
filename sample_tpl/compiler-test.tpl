struct query_state_struct {
}

fun query_init(qs: *query_state_struct) -> nil {

}

fun query_produce(qs: *query_state_struct) -> nil {
  for (r0 in test_1) {
    if (r0.colA < 500) {

    }
  }
}

fun query_teardown(qs: *query_state_struct) -> nil {

}

fun main() -> int32 {
  var query_state : query_state_struct
  query_init(&query_state)
  query_produce(&query_state)
  query_teardown(&query_state)
  return 0
}