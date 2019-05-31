#pragma once

#include <algorithm>
#include <limits>

#include "execution/sql/value.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {

// ---------------------------------------------------------
// Count
// ---------------------------------------------------------

/**
 * Count aggregate (skips nulls)
 */
class CountAggregate {
 public:
  /**
   * Constructor
   */
  CountAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CountAggregate);

  /**
   * Advance the count based on the NULLness of the input value
   */
  void Advance(const Val *val) { count_ += static_cast<u64>(!val->is_null); }

  /**
   * Merge this count with the \a that count
   */
  void Merge(const CountAggregate &that) { count_ += that.count_; }

  /**
   * Reset the aggregate
   */
  void Reset() { count_ = 0; }

  /**
   * Return the current value of the count
   */
  Integer GetCountResult() const { return Integer(count_); }

 private:
  u64 count_{0};
};

// ---------------------------------------------------------
// Count Star
// ---------------------------------------------------------
/**
 * Count start aggregate
 */
class CountStarAggregate {
 public:
  /**
   * Constructor
   */
  CountStarAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CountStarAggregate);

  /**
   * Advance the aggregate by one
   */
  void Advance(UNUSED const Val *val) { count_++; }

  /**
   * Merge this count with the \a that count
   */
  void Merge(const CountStarAggregate &that) { count_ += that.count_; }

  /**
   * Reset the aggregate
   */
  void Reset() { count_ = 0; }

  /**
   * Return the current value of the count
   */
  Integer GetCountResult() const { return Integer(count_); }

 private:
  u64 count_{0};
};

// ---------------------------------------------------------
// Sums
// ---------------------------------------------------------

/**
 * Base class for Sums
 */
class NullableAggregate {
 public:
  /**
   * Constructor
   */
  NullableAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(NullableAggregate);

  /**
   * Increment the number of tuples this aggregate has seen
   */
  void IncrementUpdateCount() { num_updates_++; }

  /**
   * Reset
   */
  void ResetUpdateCount() { num_updates_ = 0; }

  /**
   * Merge this sum with the one provided
   */
  void Merge(const NullableAggregate &that) { num_updates_ += that.num_updates_; }

  /**
   * Return the number of times this aggregate has been updated
   */
  u64 GetNumUpdates() const { return num_updates_; }

 private:
  u64 num_updates_{0};
};

/**
 * Integer Sums
 */
class IntegerSumAggregate : public NullableAggregate {
 public:
  /**
   * Constructor
   */
  IntegerSumAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IntegerSumAggregate);

  /**
   * Advance the aggregate by the non-nullable input value @em val
   */
  void Advance(const Integer *val) {
    TPL_ASSERT(!val->is_null, "Received NULL input in non-NULLable aggregator!");
    IncrementUpdateCount();
    sum_ += val->val;
  }

  /**
   * Advance the aggregate by the nullable input value @em val
   */
  void AdvanceNullable(const Integer *val) {
    if (!val->is_null) {
      Advance(val);
    }
  }

  /**
   * Merge another aggregate in
   */
  void Merge(const IntegerSumAggregate &that) {
    NullableAggregate::Merge(that);
    Integer i = that.GetResultSum();
    if (!i.is_null) {
      sum_ += i.val;
    }
  }

  /**
   * Reset the aggregate
   */
  void Reset() {
    ResetUpdateCount();
    sum_ = 0;
  }

  /**
   * Return the result of the summation
   */
  Integer GetResultSum() const {
    Integer sum(sum_);
    sum.is_null = (GetNumUpdates() == 0);
    return sum;
  }

 private:
  i64 sum_{0};
};

// ---------------------------------------------------------
// Max
// ---------------------------------------------------------

/**
 * Integer Max
 */
class IntegerMaxAggregate : public NullableAggregate {
 public:
  /**
   * Constructor
   */
  IntegerMaxAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IntegerMaxAggregate);

  /**
   * Advance the aggregate by the non-nullable input value @em val
   */
  void Advance(const Integer *val) {
    TPL_ASSERT(!val->is_null, "Received NULL input in non-NULLable aggregator!");
    IncrementUpdateCount();
    max_ = std::max(val->val, max_);
  }

  /**
   * Advance the aggregate by the nullable input value @em val
   */
  void AdvanceNullable(const Integer *val) {
    if (!val->is_null) {
      Advance(val);
    }
  }

  /**
   * Merge another aggregate in
   */
  void Merge(const IntegerMaxAggregate &that) {
    NullableAggregate::Merge(that);
    Integer i = that.GetResultMax();
    if (!i.is_null) {
      max_ = std::max(i.val, max_);
    }
  }

  /**
   * Reset the aggregate
   */
  void Reset() {
    ResetUpdateCount();
    max_ = std::numeric_limits<i64>::min();
  }

  /**
   * Return the result of the max
   */
  Integer GetResultMax() const {
    Integer max(max_);
    max.is_null = (GetNumUpdates() == 0);
    return max;
  }

 private:
  i64 max_{std::numeric_limits<i64>::min()};
};

// ---------------------------------------------------------
// Min
// ---------------------------------------------------------

/**
 * Integer Min
 */
class IntegerMinAggregate : public NullableAggregate {
 public:
  /**
   * Constructor
   */
  IntegerMinAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IntegerMinAggregate);

  /**
   * Advance the aggregate by the non-nullable input value @em val
   */
  void Advance(const Integer *val) {
    TPL_ASSERT(!val->is_null, "Received NULL input in non-NULLable aggregator!");
    IncrementUpdateCount();
    min_ = std::min(val->val, min_);
  }

  /**
   * Advance the aggregate by the nullable input value @em val
   */
  void AdvanceNullable(const Integer *val) {
    if (!val->is_null) {
      Advance(val);
    }
  }

  /**
   * Merge another aggregate in
   */
  void Merge(const IntegerMinAggregate &that) {
    NullableAggregate::Merge(that);
    Integer i = that.GetResultMin();
    if (!i.is_null) {
      min_ = std::min(i.val, min_);
    }
  }

  /**
   * Reset the aggregate
   */
  void Reset() {
    ResetUpdateCount();
    min_ = std::numeric_limits<i64>::max();
  }

  /**
   * Return the result of the minimum
   */
  Integer GetResultMin() const {
    Integer min(min_);
    min.is_null = (GetNumUpdates() == 0);
    return min;
  }

 private:
  i64 min_{std::numeric_limits<i64>::max()};
};

// ---------------------------------------------------------
// Average
// ---------------------------------------------------------

/**
 * Integer Avg
 */
class IntegerAvgAggregate : public IntegerSumAggregate {
 public:
  /**
   * Constructor
   */
  IntegerAvgAggregate() = default;

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(IntegerAvgAggregate);

  /**
   * Return the result of the minimum
   */
  Integer GetResultAvg() const {
    u64 num_updates = GetNumUpdates();
    if (num_updates == 0) {
      return Integer::Null();
    }

    Integer avg_result = GetResultSum().Divide(Integer(num_updates));
    return avg_result;
  }
};

}  // namespace tpl::sql
