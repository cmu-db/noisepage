#pragma once

#include <algorithm>
#include <limits>

#include "common/macros.h"
#include "execution/sql/value.h"

namespace noisepage::execution::sql {

/** Counting aggregate. */
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
  void Advance(const Val &val) { count_ += static_cast<uint64_t>(!val.is_null_); }

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
  uint64_t count_{0};
};

/** COUNT(*) aggregate. */
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
  void Advance(UNUSED_ATTRIBUTE const Val &val) { count_++; }

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
  uint64_t count_{0};
};

/** Generic summations. */
template <typename T>
class SumAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Template type must subclass value");

 public:
  /**
   * Constructor.
   */
  SumAggregate() : sum_(static_cast<decltype(T::val_)>(0)) { sum_.is_null_ = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(SumAggregate);

  /**
   * Advance the aggregate by a given input value.
   * If the input is NULL, no change is applied to the aggregate.
   * @param val The (potentially NULL) value to advance the sum by.
   */
  void Advance(const T &val) {
    if (val.is_null_) {
      return;
    }
    sum_.is_null_ = false;
    sum_.val_ += val.val_;
  }

  /**
   * Merge a partial sum aggregate into this aggregate.
   * If the partial sum is NULL, no change is applied to this aggregate.
   * @param that The (potentially NULL) value to merge into this aggregate.
   */
  void Merge(const SumAggregate<T> &that) {
    if (that.sum_.is_null_) {
      return;
    }
    sum_.is_null_ = false;
    sum_.val_ += that.sum_.val_;
  }

  /**
   * Reset the summation.
   */
  void Reset() {
    sum_.is_null_ = true;
    sum_.val_ = 0;
  }

  /**
   * Return the result of the summation.
   * @return The current value of the sum.
   */
  const T &GetResultSum() const { return sum_; }

 private:
  T sum_;
};

/** Integer sums. */
class IntegerSumAggregate : public SumAggregate<Integer> {};

/** Real sums. */
class RealSumAggregate : public SumAggregate<Real> {};

/** Generic max. */
template <typename T>
class MaxAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Template type must subclass value");

 public:
  /**
   * Constructor.
   */
  MaxAggregate() : max_(std::numeric_limits<decltype(T::val_)>::min()) { max_.is_null_ = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MaxAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const T &val) {
    if (val.is_null_) {
      return;
    }

    if (max_.is_null_) {  // Initial null value should not be larger than any value
      max_.val_ = val.val_;
    } else {
      max_.val_ = std::max(val.val_, max_.val_);
    }
    max_.is_null_ = false;
  }

  /**
   * Merge a partial max aggregate into this aggregate.
   */
  void Merge(const MaxAggregate<T> &that) {
    if (that.max_.is_null_) {
      return;
    }

    if (max_.is_null_) {  // Initial null value should not be larger than any value
      max_.val_ = that.max_.val_;
    } else {
      max_.val_ = std::max(that.max_.val_, max_.val_);
    }
    max_.is_null_ = false;
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    max_.is_null_ = true;
    max_.val_ = std::numeric_limits<decltype(T::val_)>::min();
  }

  /**
   * Return the result of the max.
   */
  const T &GetResultMax() const { return max_; }

 private:
  T max_;
};

/** Integer max. */
class IntegerMaxAggregate : public MaxAggregate<Integer> {};

/** Real max. */
class RealMaxAggregate : public MaxAggregate<Real> {};

/** Date max. */
class DateMaxAggregate : public MaxAggregate<DateVal> {};

/** Timestamp max. */
class TimestampMaxAggregate : public MaxAggregate<TimestampVal> {};

/** String max. */
class StringMaxAggregate : public MaxAggregate<StringVal> {};

/** Generic min. */
template <typename T>
class MinAggregate {
  static_assert(std::is_base_of_v<Val, T>, "Template type must subclass value");

 public:
  /**
   * Constructor.
   */
  MinAggregate() : min_(std::numeric_limits<decltype(T::val_)>::max()) { min_.is_null_ = true; }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(MinAggregate);

  /**
   * Advance the aggregate by the input value @em val.
   */
  void Advance(const T &val) {
    if (val.is_null_) {
      return;
    }

    if (min_.is_null_) {  // Initial null String should not be smaller than any string
      min_.val_ = val.val_;
    } else {
      min_.val_ = std::min(val.val_, min_.val_);
    }
    min_.is_null_ = false;
  }

  /**
   * Merge a partial min aggregate into this aggregate.
   */
  void Merge(const MinAggregate<T> &that) {
    if (that.min_.is_null_) {
      return;
    }

    if (min_.is_null_) {  // Initial null String should not be smaller than any string
      min_.val_ = that.min_.val_;
    } else {
      min_.val_ = std::min(that.min_.val_, min_.val_);
    }
    min_.is_null_ = false;
  }

  /**
   * Reset the aggregate.
   */
  void Reset() {
    min_.is_null_ = true;
    min_.val_ = std::numeric_limits<decltype(T::val_)>::max();
  }

  /**
   * Return the result of the minimum.
   */
  const T &GetResultMin() const { return min_; }

 private:
  T min_;
};

/** Integer min. */
class IntegerMinAggregate : public MinAggregate<Integer> {};

/** Real min. */
class RealMinAggregate : public MinAggregate<Real> {};

/** Date min. */
class DateMinAggregate : public MinAggregate<DateVal> {};

/** Timestamp min. */
class TimestampMinAggregate : public MinAggregate<TimestampVal> {};

/** String min. */
class StringMinAggregate : public MinAggregate<StringVal> {};

/** Average aggregate. */
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
    if (val.is_null_) {
      return;
    }
    sum_ += static_cast<double>(val.val_);
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
  uint64_t count_{0};
};

}  // namespace noisepage::execution::sql
