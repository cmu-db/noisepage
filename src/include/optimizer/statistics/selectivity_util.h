#pragma once

#include <algorithm>

#include "common/managed_pointer.h"
#include "optimizer/statistics/new_column_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class SelectivityUtil {
 public:
  /**
   * Compute selectivity of a condition
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double ComputeSelectivity(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                   const ValueCondition &condition);

  /**
   * Computes selectivity of the Less Than condition
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double LessThan(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
    double res = LessThanOrEqualTo(column_stats, condition) - Equal(column_stats, condition);
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Less Than Or Equal To Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double LessThanOrEqualTo(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                  const ValueCondition &condition);

  /**
   * Computes Greater Than Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double GreaterThan(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
    double res = 1 - LessThanOrEqualTo(column_stats, condition) - column_stats->GetFracNull();
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Greater Than Or Equal To Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double GreaterThanOrEqualTo(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                     const ValueCondition &condition) {
    double res = 1 - LessThan(column_stats, condition) - column_stats->GetFracNull();
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Equal Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double Equal(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition);

  /**
   * Computes Not Equal Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double NotEqual(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
    double res = 1 - Equal(column_stats, condition) - column_stats->GetFracNull();
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes LIKE Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   *
   * Peloton: Complete implementation once we support LIKE Operator
   */
  template <typename T>
  static double Like(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition);

  /**
   * Computes Not LIKE Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double NotLike(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
    return 1 - Like(column_stats, condition);
  }

  /**
   * Computes In Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double In(UNUSED_ATTRIBUTE common::ManagedPointer<NewColumnStats<T>> column_stats,
                   UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY;
  }

  /**
   * Computes Distinct From Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double DistinctFrom(UNUSED_ATTRIBUTE common::ManagedPointer<NewColumnStats<T>> column_stats,
                             UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY;
  }
};

template <typename T>
double SelectivityUtil::ComputeSelectivity(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                           const ValueCondition &condition) {
  switch (condition.GetType()) {
    case parser::ExpressionType::COMPARE_LESS_THAN:
      return LessThan(column_stats, condition);
    case parser::ExpressionType::COMPARE_GREATER_THAN:
      return GreaterThan(column_stats, condition);
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      return LessThanOrEqualTo(column_stats, condition);
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      return GreaterThanOrEqualTo(column_stats, condition);
    case parser::ExpressionType::COMPARE_EQUAL:
      return Equal(column_stats, condition);
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
      return NotEqual(column_stats, condition);
    case parser::ExpressionType::COMPARE_LIKE:
      return Like(column_stats, condition);
    case parser::ExpressionType::COMPARE_NOT_LIKE:
      return NotLike(column_stats, condition);
    case parser::ExpressionType::COMPARE_IN:
      return In(column_stats, condition);
    case parser::ExpressionType::COMPARE_IS_DISTINCT_FROM:
      return DistinctFrom(column_stats, condition);
    default:
      OPTIMIZER_LOG_WARN("Expression type {0} not supported for computing selectivity",
                         ExpressionTypeToString(condition.GetType(), false).c_str());
      return DEFAULT_SELECTIVITY;
  }
}

template <typename T>
double SelectivityUtil::LessThanOrEqualTo(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                          const ValueCondition &condition) {
  const auto value = condition.GetPointerToValue()->Peek<decltype(T::val_)>();

  // Return default selectivity if empty column_stats
  if (column_stats == nullptr) {
    return DEFAULT_SELECTIVITY;
  }

  // Use histogram to estimate selectivity
  auto histogram = column_stats->GetHistogram();

  if (value <= histogram->GetMinValue()) return 0;
  if (value > histogram->GetMaxValue()) return 1;
  double res =
      static_cast<double>(histogram->EstimateItemCount(value)) / static_cast<double>(column_stats->GetNumRows());
  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "res must be within valid range");
  return res;
}

template <typename T>
double SelectivityUtil::Equal(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
  // Convert value type to raw value (double)
  const auto value = condition.GetPointerToValue()->Peek<decltype(T::val_)>();
  if (column_stats == nullptr) {
    OPTIMIZER_LOG_DEBUG("column_stats pointer passed is null");
    return DEFAULT_SELECTIVITY;
  }

  size_t numrows = column_stats->GetNumRows();

  // For now only double is supported in stats storage
  auto top_k = column_stats->GetTopK();

  // Find frequency of the value if present in the top K elements.
  auto value_frequency_estimate = top_k->EstimateItemCount(value);

  double res = value_frequency_estimate / static_cast<double>(numrows);

  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "res must be in valid range");
  return res;
}

// Selectivity for 'LIKE' operator. The column type must be VARCHAR.
// Complete implementation once we support LIKE operator.
template <typename T>
double SelectivityUtil::Like(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
  // Check whether column type is VARCHAR.
  if ((condition.GetPointerToValue())->GetReturnValueType() != type::TypeId::VARCHAR) {
    return DEFAULT_SELECTIVITY;
  }

  if (column_stats == nullptr) {
    return DEFAULT_SELECTIVITY;
  }

  /*
  TODO(wz2): Enable once ExecutionEngine for sampling LIKE selectivity is ready
  auto column_stats = column_stats->GetColumnStats(condition.GetColumnID());
  size_t matched_count = 0;
  size_t total_count = 0;

  // Sample on the fly
  const char *pattern = (condition.value).GetData();
  oid_t column_id = column_stats->column_id;

  auto sampler = column_stats->GetSampler();
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
