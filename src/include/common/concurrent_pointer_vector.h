#pragma once

#include <common/macros.h>
#include <common/shared_latch.h>
#include <execution/util/execution_common.h>
#include <tbb/mutex.h>
#include <condition_variable>  // NOLINT

namespace terrier::common {

/**
 * A concurrent Vector of pointers that supports inserts and accesses at an index
 */
template <class T>
class ConcurrentPointerVector {
 public:
  /**
   * ConcurrentPointerVector constuctor
   *
   * @param start_size optional initial allocation size for the vector. Default is START_SIZE
   */
  explicit ConcurrentPointerVector(uint64_t start_size = START_SIZE)
      : capacity_(start_size == 0 ? 1 : start_size), claimable_index_(0), first_not_readable_index_(0) {
    array_ = new T *[capacity_];
    for (uint64_t i = 0; i < capacity_; i++) SetNotReadable(array_, i);
  }

  /**
   * ~ConcurrentPointerVector deconstructor
   */
  ~ConcurrentPointerVector() { delete[] array_; }

  /**
   * Insert
   *
   * Each new item atomically claims an index into which it will insert.
   * Inserts handle resizes, and updating readability flags.
   *
   * @param item pointer to item to be inserted
   * @return index of item in vector
   */
  uint64_t Insert(T *item) {
    // claims index in vector
    uint64_t my_index;
    do {
      my_index = claimable_index_;
    } while (!claimable_index_.compare_exchange_strong(my_index, my_index + 1));

    // resize vector
    if (UNLIKELY(my_index == capacity_)) {
      // allocate new vector and mark as not readable all new slots
      T **new_array = new T *[capacity_ * RESIZE_FACTOR];
      for (uint64_t i = capacity_; i < capacity_ * RESIZE_FACTOR; i++) SetNotReadable(new_array, i);

      // copy over all the readable values from the front of the vector into the new_array
      T **old_array;
      uint64_t i;
      {
        common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
        old_array = array_;
        for (i = 0; i < capacity_ && i < first_not_readable_index_; i++) {
          new_array[i] = array_[i];
        }
      }

      // copy over all remaining values into the new_array
      {
        common::SharedLatch::ScopedExclusiveLatch l(&array_pointer_latch_);
        for (; i < capacity_; i++) {
          new_array[i] = array_[i];
        }
        array_ = new_array;
      }

      delete[] old_array;

      // change capacity and notify threads waiting on resize
      {
        std::unique_lock<std::mutex> l(resize_mutex_);
        capacity_ = capacity_ * RESIZE_FACTOR;
        resize_cv_.notify_all();
      }

    } else if (my_index > capacity_) {
      // must wait for resize
      std::unique_lock<std::mutex> l(resize_mutex_);
      resize_cv_.wait(l, [&] { return my_index < capacity_; });
    }

    TERRIER_ASSERT(my_index < capacity_, "must safely index into array");

    {
      // insert item and set readability
      common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
      array_[my_index] = item;
      SetReadable(array_, my_index);

      // update first_not_readable_index_ as much as possible
      // increment until index is out of range or no longer readable or some other thread has updated
      // in which case we can be done since that thread will handle the rest of the updating
      uint64_t readable_index;
      do {
        readable_index = first_not_readable_index_;
      } while (readable_index < capacity_ && GetReadability(array_, readable_index) &&
               first_not_readable_index_.compare_exchange_strong(readable_index, readable_index + 1));
    }

    return my_index;
  }

  /**
   * LookUp
   *
   * @param index to inspect vector at
   * @return pointer at desired index
   */
  T *LookUp(uint64_t index) {
    TERRIER_ASSERT(index <= claimable_index_, "vector access out of bounds");

    common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);

    // it is possible that this lookup is by iteration through an index that is not yet readable
    // in that case we just wait until it is readable
    while (UNLIKELY(!GetReadability(array_, index))) {
    }

    // remove the readability flag
    return reinterpret_cast<T *>(reinterpret_cast<uint64_t>(array_[index]) & (~READABLE_FLAG));
  }

  /**
   * @return size of the vector which is the current claimable vector
   */
  uint64_t size() const { return claimable_index_; }  // NOLINT

  /**
   * Iterator iterator for the vector, Snapshot compliant
   */
  class Iterator {
   public:
    /**
     * @return underlying pointer at the spot of iteration
     */
    T *operator*() {
      TERRIER_ASSERT(!is_end_, "vector access out of bounds");
      return vector_->LookUp(current_index_);
    }
    /**
     * @return pointer to current spot of iteration
     */
    T **operator->() {
      local_value_ = operator*();
      return &local_value_;
    }

    /**
     * pre-fix increment.
     * @return self-reference after the iterator is advanced
     */
    Iterator &operator++() {
      TERRIER_ASSERT(current_index_ < end_index_, "vector access out of bounds");
      current_index_++;
      return *this;
    }

    /**
     * post-fix increment.
     * @return copy of the iterator equal to this before increment
     */
    Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    /**
     * Equality check.
     * @param other other iterator to compare to
     * @return if the two iterators point to the same spot
     */
    bool operator==(const Iterator &other) const {
      if (LIKELY(other.is_end_)) {
        return current_index_ >= end_index_;
      }
      if (LIKELY(is_end_)) {
        return other.current_index_ >= other.end_index_;
      }
      TERRIER_ASSERT(vector_ == other.vector_, "should only compare iterators for the same vector");
      return current_index_ == other.current_index_;
    }
    /**
     * Equality check.
     * @param other other iterator to compare to
     * @return if the two iterators point to different spot
     */
    bool operator!=(const Iterator &other) const { return !operator==(other); }

    /**
     * Iterator default constructor, initializes to end of the vector
     */
    Iterator() = default;

    /**
     * Iterator constructor for iterator of certain vector, initializes to start of vector, and iterates to length
     * of vector at time of creation
     */
    Iterator(const ConcurrentPointerVector *vector)  // NOLINT
        : end_index_(vector->claimable_index_),
          vector_(const_cast<ConcurrentPointerVector *>(vector)),
          is_end_(false) {}
    ~Iterator() = default;

   private:
    uint64_t current_index_ = 0, end_index_ = 0;
    ConcurrentPointerVector *vector_ = nullptr;
    T **local_value_ = nullptr;
    bool is_end_ = true;
  };

  /**
   * begin
   * @return Iterator at the begining of vector
   */
  Iterator begin() const { return {this}; }  // NOLINT

  /**
   * @return Iterator at the end of vector
   */
  Iterator end() const { return end_; }  // NOLINT

 protected:
  /**
   * RESIZE_FACTOR factor of resize when capacity is reached in vector
   */
  static const uint64_t RESIZE_FACTOR = 2;

  /**
   * START_SIZE default initial size of vector
   */
  static const uint64_t START_SIZE = 8;

 private:
  const Iterator end_ = {};
  static const uint64_t SHIFT_AMOUNT = 63;
  static const uint64_t READABLE_FLAG = static_cast<uint64_t>(1) << SHIFT_AMOUNT;
  // the first bit in the pointer is used as a flag to denote whether an item has been inserted into this
  void SetReadable(T **array, uint64_t i) {
    array[i] = reinterpret_cast<T *>(reinterpret_cast<uint64_t>(array[i]) | READABLE_FLAG);
  }

  void SetNotReadable(T **array, uint64_t i) {
    array[i] = reinterpret_cast<T *>(reinterpret_cast<uint64_t>(array[i]) & ~static_cast<uint64_t>(READABLE_FLAG));
  }

  bool GetReadability(T **array, uint64_t i) const {
    return static_cast<bool>(reinterpret_cast<uint64_t>(array[i]) >> SHIFT_AMOUNT);
  }

  std::condition_variable resize_cv_;
  std::atomic<uint64_t> capacity_;
  std::atomic<uint64_t> claimable_index_;
  std::atomic<uint64_t> first_not_readable_index_;
  T **array_;
  common::SharedLatch array_pointer_latch_;
  std::mutex resize_mutex_;
};

}  // namespace terrier::common
