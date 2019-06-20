#pragma once

#include <murmur3/MurmurHash3.h>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
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
   * This is how we will represent the internal table elements in this sketch.
   */
  using SketchElemType = uint64_t;

  /**
   * Constructor with specific sketch size.
   * @param depth
   * @param width
   */
  CountMinSketch(int depth, int width)
      : depth_{depth},
        width_{width},
        eps_{std::exp(1.0) / static_cast<double>(width)},
        gamma_{std::exp(static_cast<double>(-depth))},
        size_{0},
        total_count_{0} {
    InitTable();
  }

  /**
   * Constructor with specific error bound.
   * @param eps
   * @param gamma
   */
  CountMinSketch(double eps, double gamma)
      : depth_{static_cast<int>(ceil(log(1.0 / gamma)))},
        width_{static_cast<int>(ceil(exp(1.0) / eps))},
        eps_{eps},
        gamma_{gamma},
        size_{0},
        total_count_{0} {
    InitTable();
  }

  /**
   * Increase the count for a key by a given amount.
   * The key does not need to exist in the sketch first.
   * @param key the key to increment the count for
   * @param count how much to increment the key's count.
   */
  void Add(KeyType key, unsigned int count) {
    std::vector<int> bins = GetHashBins(key);
    uint64_t former_min = UINT64_MAX;
    for (int i = 0; i < depth_; i++) {
      former_min = std::min(table_[i][bins[i]], former_min);
      table_[i][bins[i]] += count;
    }
    if (former_min == 0) {
      ++size_;
    }
    total_count_ += count;
  }

  /**
   * Remove the count for a key by a given amount.
   * @param key the key to decrement the count for
   * @param count how much to decrement the key's count.
   */
  void Remove(KeyType key, unsigned int count) {
    std::vector<int> bins = GetHashBins(key);
    uint64_t former_min = UINT64_MAX, latter_min = UINT64_MAX;
    for (int i = 0; i < depth_; i++) {
      former_min = std::min(table_[i][bins[i]], former_min);
      if (table_[i][bins[i]] > count)
        table_[i][bins[i]] -= count;
      else
        table_[i][bins[i]] = 0;

      latter_min = std::min(latter_min, table_[i][bins[i]]);
    }
    if (former_min != 0 && latter_min == 0) {
      --size_;
    }
    total_count_ -= count;
  }

  /**
   * Compute the approximate count for the given key.
   * @param key the key to get the count for.
   * @return the approximate count number for the key.
   */
  uint64_t EstimateItemCount(KeyType key) {
    uint64_t count = UINT64_MAX;
    std::vector<int> bins = GetHashBins(key);
    for (int i = 0; i < depth_; i++) {
      count = std::min(count, table_[i][bins[i]]);
    }
    // FIXME OPTIMIZER_LOG_TRACE("Item count: %" PRIu64, count);
    return count;
  }

  /**
   * @return the number of bucket levels in this sketch
   */
  const int GetDepth() const { return depth_; }

  /**
   * @return the number of 'slots' in each bucket level in this sketch.
   */
  const int GetWidth() const { return width_; }

  /**
   * @return the error range of this sketch
   */
  const double GetErrorRange() const { return eps_; }

  /**
   * @return the error probability of this sketch.
   */
  const double GetErrorProbability() const { return gamma_; }

  /**
   * @return the approximate number of unique keys that this sketch has seen.
   */
  int GetApproximateSize() const { return size_; }

  /**
   * @return the total of the number counts this sketch has seen.
   */
  size_t GetTotalCount() const { return total_count_; }

 private:
  /**
   * Initialize the internal data table
   */
  void InitTable() {
    TERRIER_ASSERT((0.01 < eps_) && (eps_ < 1.0), "Invalid error range");
    TERRIER_ASSERT((0.0 < gamma_) && (gamma_ < 1.0), "Invalid error probability");
    TERRIER_ASSERT(depth_ > 0, "Invalid depth");
    TERRIER_ASSERT(width_ > 0, "Invalid width");

    table_ = std::vector<std::vector<SketchElemType>>(depth_, std::vector<SketchElemType>(width_));
  }

  /**
   * For the given key, return a vector with the offsets where we should
   * update this keys count information.
   * @param key the target key
   * @return vector of hash bin offsets
   */
  std::vector<int> GetHashBins(KeyType key) {
    std::vector<int> bins(depth_);
    int32_t h1 = murmur3::MurmurHash3_x64_128(key, 0);
    int32_t h2 = murmur3::MurmurHash3_x64_128(key, h1);
    for (int i = 0; i < depth_; i++) {
      bins.push_back(std::abs((h1 + i * h2) % width_));
    }
    return bins;
  }

  /**
   * The number of bucket levels in our sketch
   */
  const int depth_;

  /**
   * The number of 'slots' in each bucket level of the sketch.
   */
  const int width_;

  /**
   * Error Range (0.01 < eps < 1)
   */
  const double eps_;

  /**
   * The probability of error (the smaller the better) 0 < gamma < 1
   */
  const double gamma_;

  /**
   * The approximate number of keys in our sketch
   */
  int size_;

  /**
   * Simple counter of the number of entries we have stored.
   * This will always be accurate.
   */
  size_t total_count_;

  /**
   * The internal table where we store the approximate counts
   */
  std::vector<std::vector<SketchElemType>> table_;
};

}  // namespace terrier::optimizer
