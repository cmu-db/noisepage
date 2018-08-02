#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <functional>
#include <utility>
#include "common/macros.h"
#include "common/typedefs.h"

namespace terrier {
#define TEMPLATE_ARGS K, V, Hasher, Equality, Alloc
/**
 * A thread-safe map implementation. For the time being make sure that the value
 * types are trivially copyable value types (ints, pointers, reference, etc.)
 * @tparam K key type
 * @tparam V value type
 * @tparam Hasher hasher used for keys.
 * @tparam Equality equality check used for keys
 * @tparam Alloc Allocator type used
 */
template <typename K, typename V, typename Hasher = tbb::tbb_hash<K>, typename Equality = std::equal_to<K>,
          typename Alloc = tbb::tbb_allocator<std::pair<const K, V>>>
class ConcurrentMap {
  // This wrapper is here so we are free to swap out underlying implementation
  // of the data structure or hand-craft it ourselves. Compiler should inline
  // most of it for us anyway and incur minimal overhead. (Currently using tbb
  // see https://software.intel.com/en-us/node/506171)
  //
  // Keep the interface minimalistic until we figure out what implementation to use.
 public:
  /**
   * Iterator type for the map
   */
  class Iterator {
    using val = std::pair<const K, V>;

   public:
    /**
     * Wraps around a tbb iterator. Subject to change if we change implementation
     * @param it iterator of the underlying map
     */
    explicit Iterator(typename tbb::concurrent_unordered_map<TEMPLATE_ARGS>::iterator it) : it_(it) {}

    /**
     * @return reference to the underlying value
     */
    val &operator*() const { return it_.operator*(); }

    /**
     * @return pointer to the underlying value
     */
    val *operator->() const { return it_.operator->(); }

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
    typename tbb::concurrent_unordered_map<TEMPLATE_ARGS>::iterator it_;
  };

  /**
   *
   * @return
   */
  template <typename... Args>
  std::pair<Iterator, bool> Emplace(Args &&... args) {
    auto result = map_.emplace(std::move(args)...);
    return {Iterator(result.first), result.second};
  }

  /**
   * Insert the specified key and value into the map. Overwrites mapping if a
   * mapping already exists.
   * @param key key to insert
   * @param value value to insert
   * @return whether the insertion actually took place
   */
  std::pair<Iterator, bool> Insert(const K &key, V value) {
    auto result = map_.insert(std::make_pair(key, value));
    return {Iterator(result.first), result.second};
  }

  /**
   * Finds the value mapped to by the supplied key, or return false if no such
   * value exists.
   * @param key key to lookup
   * @return iterator to the element, or end
   */
  Iterator Find(const K &key) {
    auto it = map_.find(key);
    if (it == map_.end()) return End();
    return Iterator(it);
  }

  /**
   * Deletes the key and associated value if it exists (or no-op otherwise).
   * This operation is not safe to call concurrently.
   * @param key key to remove
   */
  void UnsafeErase(const K &key) { map_.unsafe_erase(key); }

  /**
   * @return Iterator to the first element
   */
  Iterator Begin() { return Iterator(map_.begin()); }

  /**
   * @return Iterator to the element following the last element
   */
  Iterator End() { return Iterator(map_.end()); }

 private:
  tbb::concurrent_unordered_map<TEMPLATE_ARGS> map_;
};
}  // namespace terrier

// TODO(Tianyu): Remove this if we don't end up using tbb
namespace tbb {
/**
 * Implements tbb_hash for StrongTypeAlias.
 * @tparam Tag a dummy class type to annotate the underlying type.
 * @tparam T the underlying type.
 */
template <class Tag, typename T>
struct tbb_hash<terrier::StrongTypeAlias<Tag, T>> {
  /**
   * Returns the TBB hash of the underlying type's contents.
   * @param alias the aliased type to be hashed.
   * @return the hash of the aliased type.
   */
  size_t operator()(const terrier::StrongTypeAlias<Tag, T> &alias) const {
    // This is fine since we know this is reference will be const to
    // the underlying tbb hash
    return tbb_hash<T>()(!alias);
  }
};
#undef TEMPLATE_ARGS
}  // namespace tbb
