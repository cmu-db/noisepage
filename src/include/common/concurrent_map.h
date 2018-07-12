#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <functional>
#include "common/common_defs.h"
#include "common/macros.h"

namespace terrier {

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
  bool Insert(const K &key, V value) {
    return map_.insert(std::make_pair(key, value)).second;
  }

  bool Find(const K &key, V &value) {
    auto it = map_.find(key);
    if (it == map_.end())
      return false;
    value = *it;
    return true;
  }

  void UnsafeErase(const K &key) {
    map_.unsafe_erase(key);
  }

 private:
  tbb::concurrent_unordered_map<K, V, Hasher, Equality, Alloc> map_;
};

// TODO(Tianyu): Remove this if we don't end up using tbb
namespace tbb {
template <class Tag, typename T> struct tbb_hash<StrongTypeAlias<Tag, T>> {
size_t operator()(const StrongTypeAlias<Tag, T> &alias) {
  return tbb_hash<T>(!alias);
}
};
}
}