#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <vector>

#include "common/managed_pointer.h"
#include "optimizer/statistics/table_stats.h"
#include "optimizer/statistics/value_condition.h"

namespace noisepage::optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

/**
 * Compute selectivity of a condition against a Table
 */
class Selectivity {
 public:
  /**
   * Compute selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double ComputeSelectivity(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition);

  /**
   * Computes Less Than Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double LessThan(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition);

  /**
   * Computes Less Than Or Equal To Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double LessThanOrEqualTo(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
    double res = LessThan(table_stats, condition) + Equal(table_stats, condition);
    return std::max(std::min(res, 1.0), 0.0);
  }

  /**
   * Computes Greater Than Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double GreaterThan(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
    return 1 - LessThanOrEqualTo(table_stats, condition);
  }

  /**
   * Computes Greater Than Or Equal To Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double GreaterThanOrEqualTo(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
    return 1 - LessThan(table_stats, condition);
  }

  /**
   * Computes Equal Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double Equal(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition);

  /**
   * Computes Not Equal Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double NotEqual(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
    return 1 - Equal(table_stats, condition);
  }

  /**
   * Computes LIKE Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   *
   * Peloton: Complete implementation once we support LIKE Operator
   */
  static double Like(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition);

  /**
   * Computes Not LIKE Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double NotLike(common::ManagedPointer<TableStats> table_stats, const ValueCondition &condition) {
    return 1 - Like(table_stats, condition);
  }

  /**
   * Computes In Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double In(UNUSED_ATTRIBUTE common::ManagedPointer<TableStats> table_stats,
                   UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY;
  }

  /**
   * Computes Distinct From Selectivity
   * @param table_stats Table Statistics
   * @param condition ValueCondition
   * @returns selectivity
   */
  static double DistinctFrom(UNUSED_ATTRIBUTE common::ManagedPointer<TableStats> table_stats,
                             UNUSED_ATTRIBUTE const ValueCondition &condition) {
    return DEFAULT_SELECTIVITY;
  }
};

}  // namespace noisepage::optimizer
