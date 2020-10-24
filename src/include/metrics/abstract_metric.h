#pragma once

#include <atomic>
#include <memory>

#include "common/spin_latch.h"
#include "metrics/abstract_raw_data.h"

namespace noisepage::metrics {
/**
 * @brief Interface representing a metric.
 *
 * To write a new Metric, first write your own RawData class, extending from
 * AbstractRawData, and extend from AbstractMetric with your RawData class as
 * template argument. Then, override the event callbacks that you wish to know
 * about. @see AbstractMetric on how to deal with concurrency.
 */

/**
 * Forward Declaration
 */
template <typename DataType>
class AbstractMetric;

/**
 * @brief Wraps around a pointer to an AbstractRawData to allow safe access.
 *
 * This class is always handed out by an AbstractMetric and would prevent an
 * Aggregator from reading or deleting the AbstractRawData it holds. When the
 * object goes out of scope, its destructor will unblock the aggregator. Access
 * to its underlying pointer is always non-blocking.
 *
 * @tparam DataType the type of AbstractRawData this Wrapper holds
 */
template <typename DataType>
class RawDataWrapper {
  friend class AbstractMetric<DataType>;

 public:
  /**
   * Unblock aggregator
   */
  ~RawDataWrapper() { latch_->Unlock(); }

  /**
   * Don't allow RawDataWrapper to be copied, only allow change of ownership (move)
   */
  DISALLOW_COPY(RawDataWrapper);

  /**
   * @return the underlying pointer
   */
  DataType *operator->() const { return ptr_; }

 private:
  /**
   * Constructs a new Wrapper instance
   * @param ptr the pointer it wraps around
   * @param latch the boolean variable it uses to signal its lifetime
   */
  RawDataWrapper(DataType *const ptr, common::SpinLatch *const latch) : ptr_(ptr), latch_(latch) {}
  DataType *const ptr_;
  common::SpinLatch *const latch_;
};

/**
 * @brief General purpose implementation to Metric that you should inherit from.
 *
 * This class implements the tricky Swap method and exposes an interface for
 * children class. @see Metric for detail
 *
 * @tparam DataType the type of AbstractRawData this Metric holds
 */
template <typename DataType>
class AbstractMetric {
 public:
  /**
   * Instantiate an abstract metric object with the templated data type
   */
  AbstractMetric() : raw_data_(new DataType()) {}
  /**
   * De-allocate pointer to raw data
   */
  ~AbstractMetric() { delete raw_data_.load(); }

  /**
   * @brief Replace RawData with an empty one and return the old one.
   *
   * Data from a metric is collected first into a thread-local storage to
   * ensure efficiency and safety, and periodically aggregated by an aggregator
   * thread into meaningful statistics. However, new statistics can still come
   * in when we aggregate, resulting in race conditions. To avoid this, every
   * time the aggregator wishes to aggregate data, the RawData object is
   * extracted and a fresh one swapped in, so collection continues seamlessly
   * while the aggregator is working.
   *
   * Unless you know what you are doing, you should probably just use the one
   * implemented for you(@see AbstractMetric). Otherwise, it is guaranteed that
   * this method is only called from the aggregator thread, so it is okay to
   * block in this method. As soon as this method returns, the aggregator
   * assumes that it is safe to start reading from the data and discards the
   * data after it's done. Therefore, it is essential that any implementation
   * ensures this method does not return if the collecting thread still can
   * write to the old raw data.
   *
   * To ensure this method works as intended, be sure to use GetRawData() to
   * access the underlying raw data
   * @return a unique pointer to the old AbstractRawData
   */
  std::unique_ptr<AbstractRawData> Swap() {
    // After this point, the collector thread can not see old data on new
    // events, but will still be able to write to it, if they loaded the
    // pointer before this operation but haven't written to it yet.
    DataType *old_data = raw_data_.exchange(new DataType());
    // We will need to wait for last writer to finish before it's safe
    // to start reading the content. It is okay to block since this
    // method should only be called from the aggregator thread.
    common::SpinLatch::ScopedSpinLatch guard(&latch_);
    return std::unique_ptr<AbstractRawData>(old_data);
  }

 protected:
  /**
   * @see RawDataWrapper
   *
   * Always use this method to access the raw data within an AbstractMetric.
   * @return a RawDataWrapper object to access raw_data_
   */
  RawDataWrapper<DataType> GetRawData() {
    // latch_ should first be flipped to false before loading the raw_data_ so
    // that the aggregator would always be blocked when it tries to swap out if
    // there is a reader. At most one instance of this should be live at any
    // given time.
    latch_.Lock();
    return {raw_data_.load(), &latch_};
  }

 private:
  /**
   * Pointer to raw data
   */
  std::atomic<DataType *> raw_data_;
  /**
   * Indicate whether it is safe to read the raw data, similar to a latch
   */
  common::SpinLatch latch_;
};
}  // namespace noisepage::metrics
