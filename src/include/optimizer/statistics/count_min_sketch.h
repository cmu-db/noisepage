#pragma once

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"
#include "murmur3/MurmurHash3.h"

namespace terrier::optimizer {

/**
 * An approximate counting data structure.
 * Think of this like a Bloom filter but instead of determining whether
 * a key exists in a set or not, the CountMinSketch estimates
 * the count for the given key.
 * @tparam ValueType the data type of the entries we will store
 */
template <typename ValueType>
class CountMinSketch {
 public:
  using SketchElemType = uint64_t;

  /**
   * Constructor with specific sketch size.
   * @param depth
   * @param width
   * @param seed
   */
  CountMinSketch(int depth, int width, unsigned int seed)
      : depth_{depth},
        width_{width},
        eps_{exp(1) / width},
        gamma_{exp(-depth)},
        size_{0} {
    InitTable(seed);
  }

  /**
   * Constructor with specific error bound.
   * @param eps
   * @param gamma
   * @param seed
   */
  CountMinSketch(double eps, double gamma, unsigned int seed)
      : depth_{(int)ceil(log(1 / gamma))},
        width_{(int)ceil(exp(1) / eps)},
        eps_{eps},
        gamma_{gamma},
        size_{0} {
    InitTable(seed);
  }

  /**
   *
   * @param item
   * @param count
   */
  void Add(ValueType item, unsigned int count) {
    std::vector<int> bins = GetHashBins(item);
    uint64_t former_min = UINT64_MAX;
    for (int i = 0; i < depth_; i++) {
      former_min = std::min(table_[i][bins[i]], former_min);
      table_[i][bins[i]] += count;
    }
    if (former_min == 0) {
      ++size_;
    }
  }

  /**
   *
   * @param item
   * @param count
   */
  void Remove(ValueType item, unsigned int count) {
    std::vector<int> bins = GetHashBins(item);
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
  }

  /**
   *
   * @param item
   * @return
   */
  uint64_t EstimateItemCount(ValueType item) {
    uint64_t count = UINT64_MAX;
    std::vector<int> bins = GetHashBins(item);
    for (int i = 0; i < depth_; i++) {
      count = std::min(count, table_[i][bins[i]]);
    }
    // FIXME OPTIMIZER_LOG_TRACE("Item count: %" PRIu64, count);
    return count;
  }

  /**
   *
   * @return
   */
  const int GetDepth() const { return depth_; }

  /**
   *
   * @return
   */
  const int GetWidth() const { return width_; }

  /**
   *
   * @return
   */
  const double GetErrorRange() const { return eps_; }

  /**
   *
   * @return
   */
  const double GetErrorProbability() const { return gamma_; }

  /**
   * @return the number of unique values that this sketch has seen.
   */
  size_t GetSize() const { return size_; }

 private:

  /**
   * Initialize the internal data table and pre-populate the
   * row hashes vector with random values.
   * @param seed
   */
  void InitTable(unsigned int seed) {
    TERRIER_ASSERT((0.01 < eps_) && (eps_ < 1.0), "Invalid error range");
    TERRIER_ASSERT((0 < gamma_) && (gamma_ < 1.0), "Invalid error probability");
    TERRIER_ASSERT(depth_ > 0, "Invalid depth");
    TERRIER_ASSERT(width_ > 0, "Invalid width");

    table_ = std::vector<std::vector<SketchElemType>>(
        depth_, std::vector<SketchElemType>(width_));

    std::minstd_rand0 generator(seed);
    for (int i = 0; i < depth_; i++) {
      row_hashes_.push_back(generator());
    }
  }

  /**
   *
   * @param item
   * @return
   */
  std::vector<int> GetHashBins(ValueType item) {
    std::vector<int> bins;
    int32_t h1 = murmur3::MurmurHash3_x64_128(item, 0);
    int32_t h2 = murmur3::MurmurHash3_x64_128(item, h1);
//    int32_t h1 = MurmurHash3_x64_128(item, strlen(item), 0);
//    int32_t h2 = MurmurHash3_x64_128(item, strlen(item), h1);

    for (int i = 0; i < depth_; i++) {
      bins.push_back(abs((h1 + i * h2) % width_));
    }
    return bins;
  }


  /**
   *
   */
  const int depth_;

  /**
   *
   */
  const int width_;

  /**
   * error range 0.01 < ep < 1
   */
  const double eps_;

  /**
   * The probability of error (the smaller the better) 0 < gamma < 1
   */
  const double gamma_;

  /**
   *
   */
  size_t size_;

  /**
   *
   */
  std::vector<std::vector<SketchElemType>> table_;

  /**
   *
   */
  std::vector<SketchElemType> row_hashes_;

};

}  // namespace terrier::optimizer
