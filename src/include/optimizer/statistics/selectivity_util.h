#pragma once

#include "common/managed_pointer.h"
#include "optimizer/statistics/column_stats.h"
#include "optimizer/statistics/value_condition.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::optimizer {

static constexpr double DEFAULT_SELECTIVITY = 0.5;

class SelectivityUtil {
 public:
/**
 * Compute selectivity of a condition
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double ComputeSelectivity(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition);

/**
 * Computes selectivity of the Less Than condition
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double LessThan(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition);

/**
 * Computes Less Than Or Equal To Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double LessThanOrEqualTo(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition) {
			double res = LessThan(column_stats, condition) + Equal(column_stats, condition);
			return std::max(std::min(res, 1.0), 0.0);
	}

/**
 * Computes Greater Than Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double GreaterThan(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition) {
			return 1 - LessThanOrEqualTo(column_stats, condition);
	}

/**
 * Computes Greater Than Or Equal To Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double GreaterThanOrEqualTo(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition) {
			return 1 - LessThan(column_stats, condition);
	}

/**
 * Computes Equal Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double Equal(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition);

/**
 * Computes Not Equal Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double NotEqual(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition) {
			return 1 - Equal(column_stats, condition);
	}

/**
 * Computes LIKE Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 *
 * Peloton: Complete implementation once we support LIKE Operator
 */
	static double Like(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition);

/**
 * Computes Not LIKE Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double NotLike(common::ManagedPointer <ColumnStats> column_stats, const ValueCondition &condition) {
			return 1 - Like(column_stats, condition);
	}

/**
 * Computes In Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double In(UNUSED_ATTRIBUTE common::ManagedPointer <ColumnStats> column_stats,
	                 UNUSED_ATTRIBUTE const ValueCondition &condition) {
			return DEFAULT_SELECTIVITY;
	}

/**
 * Computes Distinct From Selectivity
 * @param column_stats Column Statistics
 * @param condition ValueCondition
 * @returns selectivity
 */
	static double DistinctFrom(UNUSED_ATTRIBUTE common::ManagedPointer <ColumnStats> column_stats,
	                           UNUSED_ATTRIBUTE const ValueCondition &condition) {
			return DEFAULT_SELECTIVITY;
	}
};

}  // namespace terrier::optimizer
