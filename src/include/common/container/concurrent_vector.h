#pragma once

#include <tbb/concurrent_vector.h>

namespace noisepage::common {
/**
 * A concurrent implementation of a vector.
 * @tparam T type of element in the vector.
 * @tparam Alloc allocator to be used.
 * @warning Consider the non-trivial overhead associated with a concurrent data structure before defaulting to its use.
 */
template <typename T, class Alloc = tbb::cache_aligned_allocator<T>>
class ConcurrentVector {
 public:
  /**
   * An iterator over a concurrent vector.
   */
  class Iterator {
   public:
    /**
     * Constructs a new iterator.
     * @param it type of iterator to be used.
     */
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
    Iterator operator++(int) {
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

  /**
   * Constructs a new ConcurrentVector.
   * @param size number of elements to be held in the vector.
   * @param t allocator for the elements in the vector.
   */
  explicit ConcurrentVector(uint64_t size, const T &t = T()) : vector_(size, t) {}

  /**
   * Adds a new element at the end of the vector, after its current last element.
   * @param item element to be added.
   * @return an iterator pointing to the new element.
   */
  Iterator PushBack(const T &item) { return Iterator(vector_.push_back(item)); }

  /**
   * Returns a reference to the element at position n in the vector.
   * @param index position of an element in the vector.
   * @return the element at the specified position.
   */
  T &At(uint64_t index) { return vector_.at(index); }

  /**
   * Returns a reference to the element at position n in the vector.
   * @param index position of an element in the vector.
   * @return the element at the specified position.
   */
  const T &At(uint64_t index) const { return vector_.at(index); }

  /**
   * Returns a reference to the element at position n in the vector.
   * @param index position of an element in the vector.
   * @return the element at the specified position.
   */
  T &operator[](uint64_t index) { return At(index); }

  /**
   * Returns the element at the given index.
   * @param index index of the element to be retrieved.
   * @return the element in the vector at that index.
   */
  const T &operator[](uint64_t index) const { return At(index); }

  /**
   * Returns an iterator pointing to the first element in the vector.
   * @return an iterator pointing to the first element in the vector.
   */
  Iterator begin() { return Iterator(vector_.begin()); }  // NOLINT for STL name compability

  /**
   * Returns an iterator pointing to the last element in the vector.
   * @return an iterator pointing to the last element in the vector.
   */
  Iterator end() { return Iterator(vector_.end()); }  // NOLINT for STL name compability

 private:
  tbb::concurrent_vector<T, Alloc> vector_;
};
}  // namespace noisepage::common
