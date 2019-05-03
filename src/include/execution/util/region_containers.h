#pragma once

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/util/region_allocator.h"

namespace tpl::util {

/// STL std::vectors backed by a region allocator
template <typename T>
class RegionVector : public std::vector<T, StlRegionAllocator<T>> {
 public:
  explicit RegionVector(Region *region) : std::vector<T, StlRegionAllocator<T>>(StlRegionAllocator<T>(region)) {}

  RegionVector(size_t n, Region *region) : std::vector<T, StlRegionAllocator<T>>(n, StlRegionAllocator<T>(region)) {}

  RegionVector(size_t n, const T &elem, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(n, elem, StlRegionAllocator<T>(region)) {}

  RegionVector(std::initializer_list<T> list, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(list, StlRegionAllocator<T>(region)) {}

  template <typename InputIter>
  RegionVector(InputIter first, InputIter last, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(first, last, StlRegionAllocator<T>(region)) {}
};

/// STL std::unordered_map hash maps backed by a region allocator
template <typename Key, typename Value, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class RegionUnorderedMap
    : public std::unordered_map<Key, Value, Hash, KeyEqual, StlRegionAllocator<std::pair<const Key, Value>>> {
 public:
  explicit RegionUnorderedMap(Region *region)
      : std::unordered_map<Key, Value, Hash, KeyEqual, StlRegionAllocator<std::pair<const Key, Value>>>(
            StlRegionAllocator<std::pair<const Key, Value>>(region)) {}
};

}  // namespace tpl::util
