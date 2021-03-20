#include "optimizer/statistics/selectivity_util.h"

#include "loggers/optimizer_logger.h"
#include "parser/expression_defs.h"

namespace noisepage::optimizer {
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
                         ExpressionTypeToString(condition.GetType()).c_str());
      return DEFAULT_SELECTIVITY_VALUE;
  }
}

template <typename T>
double SelectivityUtil::LessThanOrEqualTo(common::ManagedPointer<NewColumnStats<T>> column_stats,
                                          const ValueCondition &condition) {
  const auto value = condition.GetPointerToValue()->Peek<decltype(T::val_)>();

  // Return default selectivity if empty column_stats
  if (column_stats == nullptr) {
    return DEFAULT_SELECTIVITY_VALUE;
  }

  // Use histogram to estimate selectivity
  auto histogram = column_stats->GetHistogram();

  if (value < histogram->GetMinValue()) return 0;
  if (value >= histogram->GetMaxValue()) return 1.0 - column_stats->GetFracNull();
  double res =
      static_cast<double>(histogram->EstimateItemCount(value)) / static_cast<double>(column_stats->GetNumRows());
  // There is a possibility that histogram's <= estimate is lesser than it is supposed to be.
  // In the case where the estimate is smaller than estimate for equal, we adjust the selectivity to
  // that of the Equal operator.
  res = std::max(res, Equal(column_stats, condition));
  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "Selectivity of operator must be within valid range");
  return res;
}

template <typename T>
double SelectivityUtil::Equal(common::ManagedPointer<NewColumnStats<T>> column_stats, const ValueCondition &condition) {
  // Convert value type to raw value (double)
  const auto value = condition.GetPointerToValue()->Peek<decltype(T::val_)>();
  if (column_stats == nullptr) {
    OPTIMIZER_LOG_DEBUG("column_stats pointer passed is null");
    return DEFAULT_SELECTIVITY_VALUE;
  }

  size_t numrows = column_stats->GetNumRows();

  // For now only double is supported in stats storage
  auto top_k = column_stats->GetTopK();

  // Find frequency of the value if present in the top K elements.
  auto value_frequency_estimate = top_k->EstimateItemCount(value);

  double res = value_frequency_estimate / static_cast<double>(numrows);

  NOISEPAGE_ASSERT(res >= 0 && res <= 1, "Selectivity of operator must be within valid range");
  return res;
}

// Explicit instantitation of template functions.
template double SelectivityUtil::Equal<execution::sql::Real>(
    common::ManagedPointer<NewColumnStats<execution::sql::Real>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::Equal<execution::sql::Integer>(
    common::ManagedPointer<NewColumnStats<execution::sql::Integer>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::Equal<execution::sql::BoolVal>(
    common::ManagedPointer<NewColumnStats<execution::sql::BoolVal>> column_stats, const ValueCondition &condition);

template double SelectivityUtil::LessThanOrEqualTo<execution::sql::Real>(
    common::ManagedPointer<NewColumnStats<execution::sql::Real>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::LessThanOrEqualTo<execution::sql::Integer>(
    common::ManagedPointer<NewColumnStats<execution::sql::Integer>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::LessThanOrEqualTo<execution::sql::BoolVal>(
    common::ManagedPointer<NewColumnStats<execution::sql::BoolVal>> column_stats, const ValueCondition &condition);

template double SelectivityUtil::ComputeSelectivity<execution::sql::Real>(
    common::ManagedPointer<NewColumnStats<execution::sql::Real>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::ComputeSelectivity<execution::sql::Integer>(
    common::ManagedPointer<NewColumnStats<execution::sql::Integer>> column_stats, const ValueCondition &condition);
template double SelectivityUtil::ComputeSelectivity<execution::sql::BoolVal>(
    common::ManagedPointer<NewColumnStats<execution::sql::BoolVal>> column_stats, const ValueCondition &condition);
}  // namespace noisepage::optimizer
