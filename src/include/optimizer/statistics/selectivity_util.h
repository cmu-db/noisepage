#pragma once

#include <algorithm>
#include <limits>

#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/constant_value_expression.h"

namespace noisepage::optimizer {

static constexpr double DEFAULT_SELECTIVITY_VALUE = 0.5;

/**
 * A utility class for computing the selectivity (Values satisfying a condition / Total values in column)
 * of columns based on column statistics.
 * Supported operators for selectivity calculations are:
 * - EQUAL
 * - NOT EQUAL
 * - LESS THAN
 * - LESS THAN OR EQUAL TO
 * - GREATER THAN
 * - GREATER THAN OR EQUAL TO
 *
 * TODO(arvindsk) IN, LIKE, NOT LIKE, IS DISTINCT FROM.
 *
 * The functions for computing selectivity require a column statistics object.
 * This object provides necessary information like TopK and Histogram which are
 * used by this class to compute selectivity.
 */
class SelectivityUtil {
 public:
  /**
   * Compute selectivity of a condition
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @return selectivity
   */
  static double ComputeSelectivity(const TableStats &table_stats, const ValueCondition &condition);

  /**
   * Compute selectivity of a condition
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double ComputeSelectivity(common::ManagedPointer<ColumnStats<T>> column_stats,
                                   const ValueCondition &condition);

  /**
   * Computes selectivity of the Less Than condition
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double LessThan(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition) {
    if (column_stats == nullptr) return DEFAULT_SELECTIVITY_VALUE;
    double res = LessThanOrEqualTo(column_stats, condition) - Equal(column_stats, condition);
    // Leave some room for acceptable error because of double calculations.
    NOISEPAGE_ASSERT(
        res + std::numeric_limits<double>::epsilon() >= 0 && res <= 1 + std::numeric_limits<double>::epsilon(),
        "Selectivity of operator must be within valid range");
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Less Than Or Equal To Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double LessThanOrEqualTo(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition);

  /**
   * Computes Greater Than Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double GreaterThan(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition) {
    if (column_stats == nullptr) return DEFAULT_SELECTIVITY_VALUE;
    double res = 1 - LessThanOrEqualTo(column_stats, condition) - column_stats->GetFracNull();
    // Leave some room for acceptable error because of double calculations.
    NOISEPAGE_ASSERT(
        res + std::numeric_limits<double>::epsilon() >= 0 && res <= 1 + std::numeric_limits<double>::epsilon(),
        "Selectivity of operator must be within valid range");
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Greater Than Or Equal To Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double GreaterThanOrEqualTo(common::ManagedPointer<ColumnStats<T>> column_stats,
                                     const ValueCondition &condition) {
    if (column_stats == nullptr) return DEFAULT_SELECTIVITY_VALUE;
    double res = 1.0 - LessThan(column_stats, condition) - column_stats->GetFracNull();
    // Leave some room for acceptable error because of double calculations.
    NOISEPAGE_ASSERT(
        res + std::numeric_limits<double>::epsilon() >= 0 && res <= 1 + std::numeric_limits<double>::epsilon(),
        "Selectivity of operator must be within valid range");
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Equal Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double Equal(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition);

  /**
   * Computes Not Equal Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double NotEqual(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition) {
    if (column_stats == nullptr) return DEFAULT_SELECTIVITY_VALUE;
    double res = 1 - Equal(column_stats, condition) - column_stats->GetFracNull();
    // Leave some room for acceptable error because of double calculations.
    NOISEPAGE_ASSERT(
        res + std::numeric_limits<double>::epsilon() >= 0 && res <= 1 + std::numeric_limits<double>::epsilon(),
        "Selectivity of operator must be within valid range");
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
  static double Like(UNUSED_ATTRIBUTE common::ManagedPointer<ColumnStats<T>> column_stats,
                     UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY_VALUE;
  }

  /**
   * Computes Not LIKE Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double NotLike(common::ManagedPointer<ColumnStats<T>> column_stats, const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY_VALUE;
  }

  /**
   * Computes In Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double In(UNUSED_ATTRIBUTE common::ManagedPointer<ColumnStats<T>> column_stats,
                   UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY_VALUE;
  }

  /**
   * Computes Distinct From Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double DistinctFrom(UNUSED_ATTRIBUTE common::ManagedPointer<ColumnStats<T>> column_stats,
                             UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY_VALUE;
  }

  /**
   * Computes Is Null Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double IsNull(common::ManagedPointer<ColumnStats<T>> column_stats,
                       UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return column_stats->GetFracNull();
  }

  /**
   * Computes Is Not Null Selectivity
   * @param column_stats Column Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  template <typename T>
  static double IsNotNull(common::ManagedPointer<ColumnStats<T>> column_stats,
                          UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return 1 - column_stats->GetFracNull();
  }
};
}  // namespace noisepage::optimizer
