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
   * @param depth
   * @param width
   */
  CountMinSketch(uint64_t width)
      : total_count_{0} {
    TERRIER_ASSERT(width > 0, "Invalid width");
    sketch_.create(width);
  }

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for
   * @param count how much to increment the key's count.
   */
  void Add(const KeyType &key, unsigned int count) {
    // OPTIMIZER_LOG_TRACE("Add(key={0}, size={1}, count={2})", key, sizeof(key), count);
    sketch_.add(reinterpret_cast<const void *>(&key), sizeof(key), count);
    total_count_ += count;
  }

  /**
   * Remove the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param count how much to decrement the key's count.
   */
  void Remove(const KeyType &key, unsigned int count) {
    sketch_.add(reinterpret_cast<const void *>(&key), sizeof(key), -count);
    total_count_ -= count;
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
   * Return the size of the sketch in bytes.
   * @return
   */
  const size_t GetSize() const { return sketch_.table_size(); }

  /**
   * @return the total count of all keys in this sketch.
   */
  size_t GetTotalCount() const { return total_count_; }

 private:

  /**
   * Simple counter of the number of entries we have stored.
   * This will always be accurate.
   */
  size_t total_count_;

  /**
   *
   */
  madoka::Sketch sketch_;

};

}  // namespace terrier::optimizer
