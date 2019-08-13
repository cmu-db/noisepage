#pragma once

#include <algorithm>
#include <limits>

#include "execution/sql/value.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace terrier::execution::sql {

// ---------------------------------------------------------
// Count
// ---------------------------------------------------------

/**
 * Counting aggregate
 */
class CountAggregate {
 public:
  /**
   * Constructor.
   */
  CountAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CountAggregate);

  /**
   * Advance the count based on the NULL-ness of the input value.
   */
  void Advance(const Val &val) { count_ += static_cast<u64>(!val.is_null); }

  /**
   * Merge this count with the @em that count.
   */
  void Merge(const CountAggregate &that) { count_ += that.count_; }

  /**
   * Reset the aggregate.
   */
  void Reset() { count_ = 0; }

  /**
   * Return the current value of the count.
   */
  Integer GetCountResult() const { return Integer(count_); }

 private:
  u64 count_{0};
};

// ---------------------------------------------------------
// Count Star
// ---------------------------------------------------------

/**
 * COUNT(*) aggregate.
 */
class CountStarAggregate {
 public:
  /**
   * Constructor.
   */
  CountStarAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(CountStarAggregate);

  /**
   * Advance the aggregate by one.
   */
  void Advance(UNUSED const Val &val) { count_++; }

  /**
   * Merge this count with the @em that count.
   */
  void Merge(const CountStarAggregate &that) { count_ += that.count_; }

  /**
   * Reset the aggregate.
   */
  void Reset() { count_ = 0; }

  /**
   * Return the current value of the count.
   */
  Integer GetCountResult() const { return Integer(count_); }

 private:
  u64 count_{0};
};

// ---------------------------------------------------------
// Sums
// ---------------------------------------------------------

// TODO(pmenon): Sums, Min, Max between integers and reals share a lot of code.
//               Consider refactoring ...

/**
 * Integer Sums
 */
class IntegerSumAggregate {
 public:
  /**
   * Constructor.
   */
  IntegerSumAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(IntegerSumAggregate);

  /**
   * Advance the aggregate by input value @em val.
   */
  void Advance(const Integer &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    sum_ += val.val;
  }

  /**
   * Merge a partial sum aggregate into this aggregate.
   */
  void Merge(const IntegerSumAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    sum_ += that.sum_;
  }

  /**
   * Reset the summation.
   */
  void Reset() {
    null_ = true;
    sum_ = 0;
  }

  /**
   * Return the result of the summation.
   */
  Integer GetResultSum() const {
    Integer sum(sum_);
    sum.is_null = null_;
    return sum;
  }

 private:
  i64 sum_{0};
  bool null_{true};
};

/**
 * Real Sums
 */
class RealSumAggregate {
 public:
  /**
   * Constructor.
   */
  RealSumAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(RealSumAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const Real &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    sum_ += val.val;
  }

  /**
   * Merge a partial real-typed summation into this aggregate.
   */
  void Merge(const RealSumAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    sum_ += that.sum_;
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    null_ = true;
    sum_ = 0;
  }

  /**
   * Return the result of the summation.
   */
  Real GetResultSum() const {
    Real sum(sum_);
    sum.is_null = null_;
    return sum;
  }

 private:
  double sum_{0.0};
  bool null_{true};
};

// ---------------------------------------------------------
// Max
// ---------------------------------------------------------

/**
 * Integer Max
 */
class IntegerMaxAggregate {
 public:
  /**
   * Constructor.
   */
  IntegerMaxAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(IntegerMaxAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const Integer &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    max_ = std::max(val.val, max_);
  }

  /**
   * Merge a partial max aggregate into this aggregate.
   */
  void Merge(const IntegerMaxAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    max_ = std::max(that.max_, max_);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    null_ = true;
    max_ = std::numeric_limits<i64>::min();
  }

  /**
   * Return the result of the max.
   */
  Integer GetResultMax() const {
    Integer max(max_);
    max.is_null = null_;
    return max;
  }

 private:
  i64 max_{std::numeric_limits<i64>::min()};
  bool null_{true};
};

/**
 * Real Max
 */
class RealMaxAggregate {
 public:
  /**
   * Constructor.
   */
  RealMaxAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(RealMaxAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const Real &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    max_ = std::max(val.val, max_);
  }

  /**
   * Merge a partial real-typed max aggregate into this aggregate.
   */
  void Merge(const RealMaxAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    max_ = std::max(that.max_, max_);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    null_ = true;
    max_ = std::numeric_limits<double>::min();
  }

  /**
   * Return the result of the max.
   */
  Real GetResultMax() const {
    Real max(max_);
    max.is_null = null_;
    return max;
  }

 private:
  double max_{std::numeric_limits<double>::min()};
  bool null_{true};
};

// ---------------------------------------------------------
// Min
// ---------------------------------------------------------

/**
 * Integer Min
 */
class IntegerMinAggregate {
 public:
  /**
   * Constructor.
   */
  IntegerMinAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(IntegerMinAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const Integer &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    min_ = std::min(val.val, min_);
  }

  /**
   * Merge a partial min aggregate into this aggregate.
   */
  void Merge(const IntegerMinAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    min_ = std::min(that.min_, min_);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    null_ = true;
    min_ = std::numeric_limits<i64>::max();
  }

  /**
   * Return the result of the minimum.
   */
  Integer GetResultMin() const {
    Integer min(min_);
    min.is_null = null_;
    return min;
  }

 private:
  i64 min_{std::numeric_limits<i64>::max()};
  bool null_{true};
};

/**
 * Real Min
 */
class RealMinAggregate {
 public:
  /**
   * Constructor.
   */
  RealMinAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(RealMinAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const Real &val) {
    if (val.is_null) {
      return;
    }
    null_ = false;
    min_ = std::min(val.val, min_);
  }

  /**
   * Merge a partial real-typed min aggregate into this aggregate.
   */
  void Merge(const RealMinAggregate &that) {
    if (that.null_) {
      return;
    }
    null_ = false;
    min_ = std::min(that.min_, min_);
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    null_ = true;
    min_ = std::numeric_limits<double>::max();
  }

  /**
   * Return the result of the minimum.
   */
  Real GetResultMin() const {
    Real min(min_);
    min.is_null = null_;
    return min;
  }

 private:
  double min_{std::numeric_limits<double>::max()};
  bool null_{true};
};

// ---------------------------------------------------------
// Average
// ---------------------------------------------------------

/**
 * Integer Avg
 */
class AvgAggregate {
 public:
  /**
   * Constructor.
   */
  AvgAggregate() = default;

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(AvgAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  template <typename T>
  void Advance(const T &val) {
    if (val.is_null) {
      return;
    }
    sum_ += static_cast<double>(val.val);
    count_++;
  }

  /**
   * Merge a partial average aggregate into this aggregate.
   */
  void Merge(const AvgAggregate &that) {
    sum_ += that.sum_;
    count_ += that.count_;
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    sum_ = 0.0;
    count_ = 0;
  }

  /**
   * Return the result of the minimum.
   */
  Real GetResultAvg() const {
    if (count_ == 0) {
      return Real::Null();
    }
    return Real(sum_ / static_cast<double>(count_));
  }

 private:
  double sum_{0.0};
  u64 count_{0};
};

}  // namespace terrier::execution::sql
