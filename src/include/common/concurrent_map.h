#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <functional>
#include "common/common_defs.h"
#include "common/macros.h"

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
template<typename K,
    typename V,
    typename Hasher = tbb::tbb_hash<K>,
    typename Equality = std::equal_to<K>,
    typename Alloc = tbb::tbb_allocator<std::pair<const K, V>>>
class ConcurrentMap {
// This wrapper is here so we are free to swap out underlying implementation
// of the data structure or hand-craft it ourselves. Compiler should inline
// most of it for us anyway and incur minimal overhead. (Currently using tbb
// see https://software.intel.com/en-us/node/506171)
//
// Keep the interface minimalistic until we figure out what implementation to use.
 public:

  class Iterator {
    using val = std::pair<const K, V>;
   public:
    explicit Iterator(typename tbb::concurrent_unordered_map<TEMPLATE_ARGS>::iterator it)
        : it_(it) {}

    val &operator*() const {
      return it_.operator*();
    }

    val *operator->() const {
      return it_.operator->();
    }

    Iterator &operator++() {
      ++it_;
      return *this;
    }

    const Iterator operator++(int) {
      Iterator result(it_++);
      return result;
    }

    bool operator==(const Iterator &other) const {
      return it_ == other.it_;
    }

    bool operator!=(const Iterator &other) const {
      return it_ != other.it_;
    }

   private:
    typename tbb::concurrent_unordered_map<TEMPLATE_ARGS>::iterator it_;
  };

  /**
   * Insert the specified key and value into the map. Overwrites mapping if a
   * mapping already exists.
   * @param key key to insert
   * @param value value to insert
   * @return whether the insertion actually took place
   */
  bool Insert(const K &key, V value) {
    return map_.insert(std::make_pair(key, value)).second;
  }

  /**
   * Finds the value mapped to by the supplied key, or return false if no such
   * value exists.
   * @param key key to lookup
   * @param value location to write the mapped value to
   * @return whether the key exists in the map
   */
  bool Find(const K &key, V &value) {
    auto it = map_.find(key);
    if (it == map_.end())
      return false;
    value = it->second;
    return true;
  }

  /**
   * Deletes the key and associated value if it exists (or no-op otherwise).
   * This operation is not safe to call concurrently.
   * @param key key to remove
   */
  void UnsafeErase(const K &key) {
    map_.unsafe_erase(key);
  }

  Iterator Begin() {
    return Iterator(map_.begin());
  }

  Iterator End() {
    return Iterator(map_.end());
  }

 private:
  tbb::concurrent_unordered_map<TEMPLATE_ARGS> map_;
};
}

// TODO(Tianyu): Remove this if we don't end up using tbb
namespace tbb {
template<class Tag, typename T>
struct tbb_hash<terrier::StrongTypeAlias<Tag, T>> {
  size_t operator()(const terrier::StrongTypeAlias<Tag, T> &alias) const {
    // This is fine since we know this is reference will be const to
    // the underlying tbb hash
    return tbb_hash<T>()(!const_cast<terrier::StrongTypeAlias<Tag,
                                                              T> &>(alias));
  }
};
#undef TEMPLATE_ARGS
}