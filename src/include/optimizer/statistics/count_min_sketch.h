#pragma once

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <vector>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"
#include "madoka/madoka.h"

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
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method.
   * @param key the key to increment the count for.
   * @param delta how much to increment the key's count.
   */
  void Increment(const KeyType &key, const uint32_t delta) { Increment(key, sizeof(key), delta); }

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for.
   * @param key_size the length of the key's data.
   * @param delta how much to increment the key's count.
   */
  void Increment(const KeyType &key, const size_t key_size, const uint32_t delta) {
    sketch_.add(reinterpret_cast<const void *>(&key), key_size, delta);
    total_count_ += delta;
  }

  /**
   * Decrease the count for a key by a given amount.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method.
   * @param key the key to decrement the count for
   * @param delta how much to decrement the key's count.
   */
  void Decrement(const KeyType &key, const uint32_t delta) { Increment(key, sizeof(key), delta); }

  /**
   * Decrease the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param key_size the length of the key's data.
   * @param delta how much to decrement the key's count.
   */
  void Decrement(const KeyType &key, const size_t key_size, const uint32_t delta) {
    sketch_.add(reinterpret_cast<const void *>(&key), sizeof(key), -1 * delta);

    // We have to check whether the delta is greater than the total count
    // to avoid wrap around.
    total_count_ = (delta <= total_count_ ? total_count_ - delta : 0UL);
  }

  /**
   * Remove the given key from the sketch. This attempts to set
   * the value of the key in the sketch to zero.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method.
   * @param key
   */
  void Remove(const KeyType &key) { Remove(key, sizeof(key)); }

  /**
   * Remove the given key from the sketch. This attempts to set
   * the value of the key in the sketch to zero.
   * @param key
   * @param key_size
   */
  void Remove(const KeyType &key, const size_t key_size) {
    auto delta = EstimateItemCount(key);
    sketch_.set(reinterpret_cast<const void *>(&key), sizeof(key), 0);

    // The total count is going to be incorrect now because we don't
    // know whether the the original delta is accurate or not.
    // We have to check whether the delta is greater than the total count
    // to avoid wrap around.
    total_count_ = (delta <= total_count_ ? total_count_ - delta : 0UL);
  }

  /**
   * Compute the approximate count for the given key.
   * This is a convenience method for those KeyTypes that have the
   * correct size defined by the sizeof method.
   * @param key the key to get the count for.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(const KeyType &key) { return EstimateItemCount(key, sizeof(key)); }

  /**
   * Compute the approximate count for the given key.
   * @param key the key to get the count for.
   * @param key_size the length of the key's data.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(const KeyType &key, const size_t key_size) {
    return sketch_.get(reinterpret_cast<const void *>(&key), key_size);
  }

  /**
   * @return the number of 'slots' in each bucket level in this sketch.
   */
  uint64_t GetWidth() const { return sketch_.width(); }

  /**
   * @return the size of the sketch in bytes.
   */
  size_t GetSize() const { return sketch_.table_size(); }

  /**
   * @return the approximate total count of all keys in this sketch.
   */
  size_t GetTotalCount() const { return total_count_; }

 private:
  /**
   * Simple counter of the approximate number of entries we have stored.
   */
  size_t total_count_;

  /**
   * The underlying sketch implementation
   */
  madoka::Sketch sketch_;
};

}  // namespace terrier::optimizer
