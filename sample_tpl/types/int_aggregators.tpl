fun main() -> int64 {
  // Init aggregates
  var sum_agg : IntegerSumAggregate
  var count_agg : CountAggregate
  var avg_agg : IntegerAvgAggregate
  var min_agg : IntegerMinAggregate
  var max_agg : IntegerMaxAggregate
  @aggInit(&sum_agg)
  @aggInit(&count_agg)
  @aggInit(&avg_agg)
  @aggInit(&min_agg)
  @aggInit(&max_agg)

  // Make inputs
  var input1 = @intToSql(5)
  var input2 = @intToSql(3)
  var input3 = @intToSql(20)
  var input4 = @intToSql(10)

  // Test sum
  @aggAdvance(&sum_agg, &input1)
  @aggAdvance(&sum_agg, &input2)
  @aggAdvance(&sum_agg, &input3)
  @aggAdvance(&sum_agg, &input4)
  if (@aggResult(&sum_agg) != @intToSql(38)) {
    return 1
  }

  // Test count
  @aggAdvance(&count_agg, &input1)
  @aggAdvance(&count_agg, &input2)
  @aggAdvance(&count_agg, &input3)
  @aggAdvance(&count_agg, &input4)
  if (@aggResult(&count_agg) != @intToSql(4)) {
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
  if (@aggResult(&min_agg) != @intToSql(3)) {
    return 1
  }

  // Test max
  @aggAdvance(&max_agg, &input1)
  @aggAdvance(&max_agg, &input2)
  @aggAdvance(&max_agg, &input3)
  @aggAdvance(&max_agg, &input4)
  if (@aggResult(&max_agg) != @intToSql(20)) {
    return 1
  }
  return 0
}