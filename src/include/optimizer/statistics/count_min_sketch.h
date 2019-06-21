#pragma once

#include <madoka/madoka.h>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <vector>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"

namespace terrier::optimizer {

/**
 * An approximate counting data structure.
 * Think of this like a Bloom filter but instead of determining whether
 * a key exists in a set or not, the CountMinSketch estimates
 * the count for the given key.
 * @tparam KeyType the data type of the entries we will store
 */
template <typename KeyType>
class CountMinSketch {
 public:
  /**
   * Constructor with specific sketch size.
   * The larger the width then the more accurate the count estimates are,
   * but this will make the sketch's memory size larger.
   * @param width the number of 'slots' in each bucket level in this sketch.
   */
  explicit CountMinSketch(uint64_t width) : total_count_{0} {
    TERRIER_ASSERT(width > 0, "Invalid width");

    // The only thing that we need to set in madoka when we initialize the
    // sketch is its the width. You can set the seed value but the documentation
    // says that you don't really need to do that.
    // https://www.s-yata.jp/madoka/doc/cpp-api.html
    sketch_.create(width);
  }

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for
   * @param delta how much to increment the key's count.
   */
  void Add(const KeyType &key, unsigned int delta) {
    // WARNING: This doesn't work correctly if KeyType is std::string
    sketch_.add(reinterpret_cast<const void *>(&key), sizeof(key), delta);
    total_count_ += delta;
  }

  /**
   * Remove the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param delta how much to decrement the key's count.
   */
  void Remove(const KeyType &key, unsigned int delta) {
    // WARNING: This doesn't work correctly if KeyType is std::string
    sketch_.add(reinterpret_cast<const void *>(&key), sizeof(key), -delta);
    total_count_ -= delta;
  }

  /**
   * Compute the approximate count for the given key.
   * @param key the key to get the count for.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(const KeyType &key) {
    return sketch_.get(reinterpret_cast<const void *>(&key), sizeof(key));
  }

  /**
   * @return the number of 'slots' in each bucket level in this sketch.
   */
  const uint64_t GetWidth() const { return sketch_.width(); }

  /**
   * @return the size of the sketch in bytes.
   */
  const size_t GetSize() const { return sketch_.table_size(); }

  /**
   * @return the total count of all keys in this sketch.
   */
  const size_t GetTotalCount() const { return total_count_; }

 private:
  /**
   * Simple counter of the number of entries we have stored.
   * This will always be accurate.
   */
  size_t total_count_;

  /**
   * The underlying sketch implementation
   */
  madoka::Sketch sketch_;
};

}  // namespace terrier::optimizer
