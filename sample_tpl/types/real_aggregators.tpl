fun main() -> int64 {
  // Init aggregates
  var sum_agg : RealSumAggregate
  var avg_agg : RealAvgAggregate
  var min_agg : RealMinAggregate
  var max_agg : RealMaxAggregate
  @aggInit(&sum_agg)
  @aggInit(&avg_agg)
  @aggInit(&min_agg)
  @aggInit(&max_agg)

  // Make inputs
  var input1 = @floatToSql(4.5)
  var input2 = @floatToSql(3.5)
  var input3 = @floatToSql(20.75)
  var input4 = @floatToSql(9.25)

  // Test sum
  @aggAdvance(&sum_agg, &input1)
  @aggAdvance(&sum_agg, &input2)
  @aggAdvance(&sum_agg, &input3)
  @aggAdvance(&sum_agg, &input4)
  if (@aggResult(&sum_agg) != @floatToSql(38.0)) {
    return 1
  }

  // Test avg
  @aggAdvance(&avg_agg, &input1)
  @aggAdvance(&avg_agg, &input2)
  @aggAdvance(&avg_agg, &input3)
  @aggAdvance(&avg_agg, &input4)
  if (@aggResult(&avg_agg) != @floatToSql(9.5)) {
    return 1
  }

  // Test min
  @aggAdvance(&min_agg, &input1)
  @aggAdvance(&min_agg, &input2)
  @aggAdvance(&min_agg, &input3)
  @aggAdvance(&min_agg, &input4)
  if (@aggResult(&min_agg) != @floatToSql(3.5)) {
    return 1
  }

  // Test max
  @aggAdvance(&max_agg, &input1)
  @aggAdvance(&max_agg, &input2)
  @aggAdvance(&max_agg, &input3)
  @aggAdvance(&max_agg, &input4)
  if (@aggResult(&max_agg) != @floatToSql(20.75)) {
    return 1
  }
  return 0
}