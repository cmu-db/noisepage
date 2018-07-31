#pragma once

#include <tbb/concurrent_vector.h>

namespace terrier::common {
template <typename T, class Alloc = tbb::cache_aligned_allocator<T>>
class ConcurrentVector {
 public:
  class Iterator {
   public:
    explicit Iterator(typename tbb::concurrent_vector<T, Alloc>::iterator it) : it_(it) {}

    /**
     * @return reference to the underlying value
     */
    T &operator*() const { return it_.operator*(); }

    /**
     * @return pointer to the underlying value
     */
    T *operator->() const { return it_.operator->(); }

    /**
     * prefix-increment
     * @return self-reference
     */
    Iterator &operator++() {
      ++it_;
      return *this;
    }

    /**
     * postfix-increment
     * @return iterator equal to this iterator before increment
     */
    const Iterator operator++(int) {
      Iterator result(it_++);
      return result;
    }

    /**
     * Equality test
     * @param other iterator to compare to
     * @return if this is equal to other
     */
    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    /**
     * Inequality test
     * @param other iterator to compare to
     * @return if this is not equal to other
     */
    bool operator!=(const Iterator &other) const { return it_ != other.it_; }

   private:
    typename tbb::concurrent_vector<T, Alloc>::iterator it_;
  };

  ConcurrentVector() = default;

  explicit ConcurrentVector(uint64_t size, const T &t = T()) : vector_(size, t) {}

  Iterator PushBack(const T &item) { return Iterator(vector_.push_back(item)); }

  T &At(uint64_t index) { return vector_.at(index); }

  const T &At(uint64_t index) const { return vector_.at(index); }

  T &operator[](uint64_t index) { return At(index); }

  const T &operator[](uint64_t index) const { return At(index); }

  Iterator Begin() { return Iterator(vector_.begin()); }

  Iterator End() { return Iterator(vector_.end()); }

 private:
  tbb::concurrent_vector<T, Alloc> vector_;
};
}  // namespace terrier::common
