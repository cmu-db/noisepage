#pragma once

#include <common/macros.h>
#include <common/shared_latch.h>
#include <execution/util/execution_common.h>
#include <tbb/mutex.h>
#include <condition_variable> // NOLINT

namespace terrier::common {

/**
 * A concurrent Vector that supports inserts and accesses at an index
 */
template <class T>
class ConcurrentPointerVector {
 public:
  explicit ConcurrentPointerVector(uint64_t start_size = START_SIZE) : capacity_(start_size == 0 ? 1 : start_size) {
    array_ = new T *[capacity_];
    for (uint64_t i = 0; i < capacity_; i++) SetNotReadable(array_, i);
  }

  ~ConcurrentPointerVector() { delete[] array_; }

  uint64_t Insert(T *item) {
    uint64_t my_index = claimable_index_;
    while (!claimable_index_.compare_exchange_strong(my_index, my_index + 1)) my_index = claimable_index_;

    if (UNLIKELY(my_index == capacity_)) {
      T **new_array = new T *[capacity_ * RESIZE_FACTOR];
      for (uint64_t i = capacity_; i < capacity_ * RESIZE_FACTOR; i++) SetNotReadable(new_array, i);
      T **old_array;
      uint64_t i;
      {
        common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
        old_array = array_;
        for (i = 0; i < capacity_ && i < num_slots_written_; i++) {
          new_array[i] = array_[i];
        }
      }

      {
        common::SharedLatch::ScopedExclusiveLatch l(&array_pointer_latch_);
        for (; i < capacity_; i++) {
          new_array[i] = array_[i];
        }
        array_ = new_array;
      }

      delete[] old_array;

      {
        std::unique_lock<std::mutex> l(resize_mutex_);
        capacity_ = capacity_ * RESIZE_FACTOR;
        resize_cv_.notify_all();
      }

    } else if (my_index > capacity_) {
      // wait until my_index < capacity_
      std::unique_lock<std::mutex> l(resize_mutex_);
      resize_cv_.wait(l, [&] { return my_index < capacity_; });
    }

    TERRIER_ASSERT(my_index < capacity_, "must safely index into array");

    {
      common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
      array_[my_index] = item;
      SetReadable(array_, my_index);

      uint64_t readable_index;
      do {
        readable_index = num_slots_written_;
      } while (readable_index < capacity_ && GetReadability(array_, readable_index) &&
               num_slots_written_.compare_exchange_strong(readable_index, readable_index + 1));
    }

    return my_index;
  }

  T *LookUp(uint64_t index) {
    TERRIER_ASSERT(index <= claimable_index_, "vector access out of bounds");

    // take read latch on array pointer latch now that it is safe to index into array
    common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
    while (UNLIKELY(!GetReadability(array_, index))) {
      array_pointer_latch_.Unlock();
      array_pointer_latch_.LockShared();
    }

    return reinterpret_cast<T *>(reinterpret_cast<uint64_t>(array_[index]) & (~READABLE_FLAG));
  }

  uint64_t size() const { return claimable_index_; }

  class Iterator {
   public:
    T *operator*() {
      TERRIER_ASSERT(!is_end_, "vector access out of bounds");
      return vector_->LookUp(current_index_);
    }
    T **operator->() {
      local_value_ = operator*();
      return &local_value_;
    }

    Iterator &operator++() {
      TERRIER_ASSERT(current_index_ < end_index_, "vector access out of bounds");
      current_index_++;
      return *this;
    }

    Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    bool operator==(const Iterator &other) const {
      if (LIKELY(other.is_end_)) {
        return current_index_ >= end_index_;
      }
      if (LIKELY(is_end_)) {
        return other.current_index_ >= other.end_index_;
      }
      return current_index_ == other.current_index_ && LIKELY(vector_ == other.vector_);
    }
    bool operator!=(const Iterator &other) const { return !operator==(other); }

    Iterator() = default;
    Iterator(const ConcurrentPointerVector *vector) // NOLINT
        : end_index_(vector->claimable_index_), vector_(const_cast<ConcurrentPointerVector *>(vector)), is_end_(false) {}
    ~Iterator() = default;

   private:
    uint64_t current_index_ = 0, end_index_ = 0;
    ConcurrentPointerVector *vector_ = nullptr;
    T **local_value_ = nullptr;
    bool is_end_ = true;
  };

  Iterator begin() const { return {this}; }
  Iterator end() const { return END; }

 protected:
  static const uint64_t RESIZE_FACTOR = 2;
  static const uint64_t START_SIZE = 8;

 private:
  const Iterator END = {};
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
  std::atomic_uint64_t capacity_, claimable_index_ = 0, num_slots_written_ = 0;
  T **array_;
  common::SharedLatch array_pointer_latch_;
  std::mutex resize_mutex_;
};

}  // namespace terrier::common
