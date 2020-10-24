#pragma once

#include <functional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/util/region_allocator.h"

namespace noisepage::execution::util {

/**
 * STL std::vectors backed by a region allocator
 * @tparam T type of the elements
 */
template <typename T>
class RegionVector : public std::vector<T, StlRegionAllocator<T>> {
 public:
  /**
   * Constructor for empty vector
   * @param region region to use for allocation
   */
  explicit RegionVector(Region *region) : std::vector<T, StlRegionAllocator<T>>(StlRegionAllocator<T>(region)) {}

  /**
   * Constructor for vector with initial size
   * @param n initial size
   * @param region region to use for allocation
   */
  RegionVector(size_t n, Region *region) : std::vector<T, StlRegionAllocator<T>>(n, StlRegionAllocator<T>(region)) {}

  /**
   * Constructor for vector with initial size and elements
   * @param n initial size
   * @param elem default element
   * @param region region to use for allocation
   */
  RegionVector(size_t n, const T &elem, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(n, elem, StlRegionAllocator<T>(region)) {}

  /**
   * Constructor with initializer list
   * @param list initializer list with the initial elements
   * @param region region to use for allocation
   */
  RegionVector(std::initializer_list<T> list, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(list, StlRegionAllocator<T>(region)) {}

  /**
   * Constructor using another iterator to fill up this vector
   * @tparam InputIter iterator type
   * @param first beginning iterator
   * @param last ending iterator
   * @param region region to use for allocation
   */
  template <typename InputIter>
  RegionVector(InputIter first, InputIter last, Region *region)
      : std::vector<T, StlRegionAllocator<T>>(first, last, StlRegionAllocator<T>(region)) {}
};

/**
 * STL std::unordered_map hash maps backed by a region allocator
 * @tparam Key type of the keys
 * @tparam Value type of the values
 * @tparam Hash hash function
 * @tparam KeyEqual equality function
 */
template <typename Key, typename Value, typename Hash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class RegionUnorderedMap
    : public std::unordered_map<Key, Value, Hash, KeyEqual, StlRegionAllocator<std::pair<const Key, Value>>> {
 public:
  /**
   * Constructor
   * @param region region to use for allocation.
   */
  explicit RegionUnorderedMap(Region *region)
      : std::unordered_map<Key, Value, Hash, KeyEqual, StlRegionAllocator<std::pair<const Key, Value>>>(
            StlRegionAllocator<std::pair<const Key, Value>>(region)) {}
};

}  // namespace noisepage::execution::util
