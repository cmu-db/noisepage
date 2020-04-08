#pragma once

#include <common/macros.h>
#include <common/shared_latch.h>
#include <execution/util/execution_common.h>
#include <tbb/mutex.h>

#include <condition_variable>

namespace terrier::common {

/**
 * A concurrent Vector that supports inserts and accesses at an index
 */
template<class T>
class ConcurrentVector {
 public:
  ConcurrentVector(uint64_t start_size = START_SIZE) : capacity_(start_size == 0 ? 1 : start_size) {
    array_ = new std::atomic<T *>[capacity_];
  }

  ~ConcurrentVector() {
    delete[] array_;
  }

  uint64_t Insert(T *item) {

    uint64_t my_index = claimable_index_;
    while (!claimable_index_.compare_exchange_strong(my_index, my_index + 1)) my_index = claimable_index_;

    if (my_index == capacity_) {
      std::atomic<T*> *new_array = new std::atomic<T*>[capacity_ * RESIZE_FACTOR];
      std::atomic<T*> *old_array;
      {
        common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
        old_array = array_;
        uint64_t i;
        for (i = 0; i < capacity_ && i < num_slots_written_; i++) {
          new_array[i] = array_[i].load();
        }

        for (; i < capacity_; i++) {
          while (num_slots_written_ <= i) {}
          new_array[i] = array_[i].load();
        }
      }

      {
        common::SharedLatch::ScopedExclusiveLatch l(&array_pointer_latch_);
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
    }

    while (num_slots_written_.load() < my_index) {}
    num_slots_written_++;

    return my_index;
  }

  T* LookUp(uint64_t index) {
    TERRIER_ASSERT(index <= claimable_index_, "vector access out of bounds");

    // wait until requested index has been written into
    while (num_slots_written_ <= index) {}

    // take read latch on array pointer latch now that it is safe to index into array
    common::SharedLatch::ScopedSharedLatch l(&array_pointer_latch_);
    return array_[index];
  }

  uint64_t size() {
    return num_slots_written_;
  }

  class Iterator {
   public:

    T* &operator*() {
      TERRIER_ASSERT(!is_end_, "vector access out of bounds");
      local_value_ = vector_->LookUp(current_index_);
    }
    T **operator->() {
      operator*();
      return &local_value_;
    }

    Iterator &operator++() {
      TERRIER_ASSERT(current_index_ < end_index_, "vector access out of bounds");
      current_index_++;
    }

    Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    Iterator &operator--() {
      TERRIER_ASSERT(current_index_ < end_index_ && current_index_ != 0, "vector access out of bounds");
      current_index_--;
    }
    Iterator operator--(int) {
      Iterator copy = *this;
      operator--();
      return copy;
    }

    bool operator==(const Iterator &other) const {
      if (LIKELY(other.is_end_)) {
        return current_index_ >= end_index_;
      }
      if (LIKELY(is_end_)) {
        return other.current_index_ >= other.end_index_;
      }
      return current_index_ == other.current_index_;
    }
    bool operator!=(const Iterator &other) const { return !operator==(other); }

   private:
    Iterator() : vector_(nullptr), is_end_(true) {}
    Iterator(ConcurrentVector *vector) : vector_(vector), is_end_(false), end_index_(vector->num_slots_written_) {}
    ~Iterator() = default;


    uint64_t current_index_ = 0, end_index_;
    ConcurrentVector *vector_;
    T **local_value_;
    bool is_end_;
  };

 protected:
  static const uint64_t RESIZE_FACTOR = 2;
  static const uint64_t START_SIZE = 8;

 private:


  std::condition_variable resize_cv_;
  std::atomic_uint64_t capacity_, claimable_index_ = 0, num_slots_written_ = 0;
  std::atomic<std::atomic<T*>*> array_;
  common::SharedLatch array_pointer_latch_;
  std::mutex resize_mutex_;
};

}  // namespace terrier::common
