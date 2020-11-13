#include "optimizer/statistics/selectivity.h"

#include "loggers/optimizer_logger.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression_defs.h"

namespace noisepage::optimizer {

double Selectivity::ComputeSelectivity(common::ManagedPointer<TableStats> stats, const ValueCondition &condition) {
  switch (condition.GetType()) {
    case parser::ExpressionType::COMPARE_LESS_THAN:
      return LessThan(stats, condition);
    case parser::ExpressionType::COMPARE_GREATER_THAN:
      return GreaterThan(stats, condition);
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      return LessThanOrEqualTo(stats, condition);
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      return GreaterThanOrEqualTo(stats, condition);
    case parser::ExpressionType::COMPARE_EQUAL:
      return Equal(stats, condition);
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
      return NotEqual(stats, condition);
    case parser::ExpressionType::COMPARE_LIKE:
      return Like(stats, condition);
    case parser::ExpressionType::COMPARE_NOT_LIKE:
      return NotLike(stats, condition);
    case parser::ExpressionType::COMPARE_IN:
      return In(stats, condition);
    case parser::ExpressionType::COMPARE_IS_DISTINCT_FROM:
      return DistinctFrom(stats, condition);
    default:
      OPTIMIZER_LOG_WARN("Expression type {0} not supported for computing selectivity",
                         ExpressionTypeToString(condition.GetType(), false).c_str());
      return DEFAULT_SELECTIVITY;
  }
}

double Selectivity::LessThan(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
  // Convert value type to raw value (double)
  NOISEPAGE_ASSERT(condition.GetPointerToValue()->GetReturnValueType() == type::TypeId::DECIMAL,
                   "It seems like there's an assumption that it's a DECIMAL type.");
  const auto value = condition.GetPointerToValue()->Peek<double>();
  if (std::isnan(value)) {
    OPTIMIZER_LOG_TRACE("Error computing less than for non-numeric type");
    return DEFAULT_SELECTIVITY;
  }

  // Return default selectivity if no column stats for given column_id
  if (!table_stats->HasColumnStats(condition.GetColumnID())) {
    return DEFAULT_SELECTIVITY;
  }

  auto column_stats = table_stats->GetColumnStats(condition.GetColumnID());

  // Use histogram to estimate selectivity
  auto histogram = column_stats->GetHistogramBounds();
  size_t n = histogram.size();
  NOISEPAGE_ASSERT(n > 0, "Histogram must have some bounds");

  // find correspond bin using binary search
  auto it = std::lower_bound(histogram.begin(), histogram.end(), value);
  double res = static_cast<double>(it - histogram.begin()) / static_cast<double>(n);
  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "res must be within valid range");
  return res;
}

double Selectivity::Equal(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
  // Convert value type to raw value (double)
  NOISEPAGE_ASSERT(condition.GetPointerToValue()->GetReturnValueType() == type::TypeId::DECIMAL,
                   "It seems like there's an assumption that it's a DECIMAL type.");
  const auto value = condition.GetPointerToValue()->Peek<double>();
  if (std::isnan(value) || !table_stats->HasColumnStats(condition.GetColumnID())) {
    OPTIMIZER_LOG_DEBUG("Calculate selectivity: return null");
    return DEFAULT_SELECTIVITY;
  }

  auto column_stats = table_stats->GetColumnStats(condition.GetColumnID());
  size_t numrows = column_stats->GetNumRows();

  // For now only double is supported in stats storage
  auto most_common_vals = column_stats->GetCommonVals();
  auto most_common_freqs = column_stats->GetCommonFreqs();
  auto first = most_common_vals.begin(), last = most_common_vals.end();

  while (first != last) {
    // For now only double is supported in stats storage
    if (*first == value) {
      break;
    }
    ++first;
  }

  double res = DEFAULT_SELECTIVITY;
  if (first != last) {
    // the target value for equality comparison (param value) is
    // found in most common values
    size_t idx = first - most_common_vals.begin();

    res = most_common_freqs[idx] / static_cast<double>(numrows);
  } else {
    // the target value for equality comparison (parm value) is
    // NOT found in most common values
    // (1 - sum(mvf))/(num_distinct - num_mcv)
    double sum_mvf = 0;
    auto first = most_common_freqs.begin(), last = most_common_freqs.end();
    while (first != last) {
      sum_mvf += *first;
      ++first;
    }

    if (numrows == 0 || column_stats->GetCardinality() == static_cast<double>(most_common_vals.size())) {
      OPTIMIZER_LOG_TRACE("Equal selectivity division by 0.");
      return DEFAULT_SELECTIVITY;
    }

    res = (1 - sum_mvf / static_cast<double>(numrows)) /
          (column_stats->GetCardinality() - static_cast<double>(most_common_vals.size()));
  }

  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "res must be in valid range");
  return res;
}

// Selectivity for 'LIKE' operator. The column type must be VARCHAR.
// Complete implementation once we support LIKE operator.
double Selectivity::Like(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
  // Check whether column type is VARCHAR.
  if ((condition.GetPointerToValue())->GetReturnValueType() != type::TypeId::VARCHAR) {
    return DEFAULT_SELECTIVITY;
  }

  if (!table_stats->GetColumnStats(condition.GetColumnID())) {
    return DEFAULT_SELECTIVITY;
  }

  /*
  TODO(wz2): Enable once ExecutionEngine for sampling LIKE selectivity is ready
  auto column_stats = table_stats->GetColumnStats(condition.GetColumnID());
  size_t matched_count = 0;
  size_t total_count = 0;

  // Sample on the fly
  const char *pattern = (condition.value).GetData();
  oid_t column_id = column_stats->column_id;

  auto sampler = table_stats->GetSampler();
  PELOTON_ASSERT(sampler != nullptr);
  if (sampler->GetSampledTuples().empty()) {
    sampler->AcquireSampleTuples(DEFAULT_SAMPLE_SIZE);
  }
  auto &sample_tuples = sampler->GetSampledTuples();
  for (size_t i = 0; i < sample_tuples.size(); i++) {
    auto value = sample_tuples[i]->GetValue(column_id);
    PELOTON_ASSERT(value.GetTypeId() == type::TypeId::VARCHAR);
    executor::ExecutorContext dummy_context(nullptr);
    if (function::StringFunctions::Like(dummy_context, value.GetData(),
                                        value.GetLength(), pattern,
                                        condition.value.GetLength())) {
      matched_count++;
    }
  }
  total_count = sample_tuples.size();
  OPTIMIZER_LOG_TRACE("total sample size %lu matched tupe %lu", total_count, matched_count);

  return total_count == 0 ? DEFAULT_SELECTIVITY : static_cast<double>(matched_count) / static_cast<double>(total_count);
  */

  return DEFAULT_SELECTIVITY;
}

}  // namespace noisepage::optimizer
