#pragma once

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"
#include "type/type_id.h"

namespace noisepage::optimizer {

/**
 * Online histogram implementation based on this paper (referred to below as JMLR10):
 *    A Streaming Parallel Decision Tree Algorithm
 *    http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * Specifically Algorithm 1, 3, and 4.
 */
template <typename KeyType>
class Histogram {
 public:
  /**
   * Constructor.
   * @param max_bins maximum number of bins in histogram.
   */
  explicit Histogram(const uint8_t max_bins)
      : max_bins_{max_bins}, bins_{}, total_{0}, minimum_{DBL_MAX}, maximum_{DBL_MIN} {}

  /**
   * Internal representation of a point/count pair in the histogram.
   */
  class Bin {
   public:
    /**
     * Constructor
     * @param point the point that this bin represents
     * @param count the count that this bin has recorded.
     */
    Bin(double point, double count) : point_{point}, count_{count} {}

    /**
     * Merge the count from the given bin into this bin.
     * The point will be a weighted average of the two bins' points.
     * @param bin
     */
    void MergeWith(const Bin &bin) {
      double new_m = count_ + bin.count_;
      point_ = (point_ * count_ + bin.point_ * bin.count_) / new_m;
      count_ = new_m;
    }

    /**
     * Return the point that this bin represents
     * @return
     */
    double GetPoint() const { return point_; }

    /**
     * Return the count that this bin has recorded.
     * @return
     */
    double GetCount() const { return count_; }

    /**
     * Increment the bin's count by the given delta and return
     * the new value.
     * @param delta
     * @return
     */
    double Increment(double delta) {
      count_ += delta;
      return count_;
    }

    /**
     * Less-than operator (based on point)
     * @param bin the bin to compare against
     * @return true if this bin is less than the given bin
     */
    bool operator<(const Bin &bin) const { return point_ < bin.point_; }

    /**
     * Equals operator (based on point)
     * @param bin the bin to compare against
     * @return true if this bin is equal to the given bin
     */
    bool operator==(const Bin &bin) const { return point_ == bin.point_; }

    /**
     * Greater-than operator (based on point)
     * @param bin the bin to compare against
     * @return true if this bin is greater than the given bin
     */
    bool operator>(const Bin &bin) const { return point_ > bin.point_; }

    /**
     * Not equals operator (based on point)
     * @param bin the bin to compare against
     * @return true if this bin is not equal to the given bin
     */
    bool operator!=(const Bin &bin) const { return point_ != bin.point_; }

    /**
     * Pretty Print!
     * @param os
     * @param b
     * @return os
     */
    friend std::ostream &operator<<(std::ostream &os, const Bin &b) {
      os << "Bin[" << b.point_ << "]: " << b.count_;
      return os;
    }

   private:
    /**
     * The value that this bin represents.
     * This is the 'p' variable in the JMLR10 algorithm.
     */
    double point_;

    /**
     * The number of hits
     * This is the 'm' variable in the JMLR10 algorithm.
     */
    double count_;
  };

  /**
   * Update the histogram that represents the set S U {point},
   * where S is the set represented by the histogram.
   * This operation does not change the number of bins.
   * This only supports numeric types that can be converted into a double
   * @param key the key to update
   */
  void Increment(const KeyType &key) {
    auto point = static_cast<double>(key);
    Bin bin{point, 1};
    InsertBin(bin);
    if (bins_.size() > max_bins_) {
      MergeTwoBinsWithMinGap();
    }
  }

  /**
   * For the given key point (where p1 < b < pB), return an estimate
   * of the number of points in the interval [-Inf, b]
   * @param point the value point to estimate
   * @return the estimate of the # of points
   */
  double EstimateItemCount(double point) {
    if (bins_.empty()) return 0.0;

    if (point >= bins_.back().GetPoint()) {
      return total_;
    }
    if (point < bins_.front().GetPoint()) {
      return 0.0;
    }

    Bin bin{point, 1};
    int i = BinarySearch(bins_, 0, static_cast<int>(bins_.size()) - 1, bin);
    if (i < 0) {
      // -1 because we want index to be element less than b
      i = std::abs(i + 1) - 1;
    }

    double pi, pi1, mi, mi1;
    std::tie(pi, pi1, mi, mi1) = GetInterval(bins_, i);

    double mb = mi + (mi1 - mi) / (pi1 - pi) * (point - pi);

    double s = ((mi + mb) / 2.0) * ((point - pi) / (pi1 - pi));

    for (int j = 0; j < i; j++) {
      s += bins_[j].GetCount();
    }

    s = s + mi / 2;

    return s;
  }

  /**
   * Return boundary points at most size max_bins with the property that the
   * number of points between two consecutive numbers uj, uj+1 and the
   * number of data points to the left of u1 and to the right of uB is
   * equal to sum of all points / max_bins.
   */
  std::vector<double> Uniform() {
    NOISEPAGE_ASSERT(max_bins_ > 0, "# of max bins is less than one");

    std::vector<double> res{};
    if (bins_.empty() || total_ <= 0) return res;

    uint32_t i = 0;
    for (uint32_t j = 0; j < bins_.size() - 1; j++) {
      double s = (j * 1.0 + 1.0) / max_bins_ * total_;
      while (i < bins_.size() - 1 && EstimateItemCount(bins_[i + 1].GetPoint()) < s) {
        i += 1;
      }
      NOISEPAGE_ASSERT(i < bins_.size() - 1, "Invalid bin offset");
      double point_i, point_i1, count_i, count_i1;
      std::tie(point_i, point_i1, count_i, count_i1) = GetInterval(bins_, i);

      double d = s - EstimateItemCount(bins_[i].GetPoint());
      double a = count_i1 - count_i;
      double b = 2.0 * count_i;
      double c = -2.0 * d;
      double z = a != 0 ? (-b + std::sqrt(b * b - 4.0 * a * c)) / (2.0 * a) : -c / b;
      double uj = point_i + (point_i1 - point_i) * z;
      res.push_back(uj);
    }
    return res;
  }

  /**
   * @return the largest value that we have in this histogram
   */
  double GetMaxValue() const { return maximum_; }

  /**
   * @return the smallest value that we have in this histogram
   */
  double GetMinValue() const { return minimum_; }

  /**
   * @return the total number of unique values that are recorded in this histogram
   */
  double GetTotalValueCount() const { return std::floor(total_); }

  /**
   * @return the maximum number of bins that this histogram supports
   */
  uint8_t GetMaxBinSize() const { return max_bins_; }

  /**
   * Pretty Print!
   * @param os
   * @param h
   * @return os
   */
  friend std::ostream &operator<<(std::ostream &os, const Histogram<KeyType> &h) {
    os << "Histogram: "
       << "total=[" << h.total_ << "] "
       << "num_bins=[" << h.bins_.size() << "]" << std::endl;
    for (Bin b : h.bins_) {
      os << "  " << b << std::endl;
    }
    return os;
  }

 private:
  /**
   * The maximum number of bins that this histogram supports
   */
  const uint8_t max_bins_;

  /**
   * Where we store the point+count values
   */
  std::vector<Bin> bins_;

  /**
   * The total number of unique values that are recorded in this histogram
   */
  double total_;

  /**
   * The smallest value that we have in this histogram
   */
  double minimum_;

  /**
   * The largest value that we have in this histogram
   */
  double maximum_;

  /**
   * Insert a new bin into our histogram. We use our own binary search
   * on our bin vector to figure out where we want to insert this mofo.
   * This also updates our min/max boundaries.
   * @param bin the bin to insert into the histogram.
   */
  void InsertBin(const Bin &bin) {
    total_ += bin.GetCount();
    if (bin.GetPoint() < minimum_) {
      minimum_ = bin.GetPoint();
    }
    if (bin.GetPoint() > maximum_) {
      maximum_ = bin.GetPoint();
    }

    int index = BinarySearch(bins_, 0, static_cast<int>(bins_.size()) - 1, bin);

    // If we already have a bin with this value, then we will just update
    // it's counter by the count of the bin that we want to insert.
    // We do this so that we can merge bins together if necessary.
    if (index >= 0) {
      bins_[index].Increment(bin.GetCount());

      // Otherwise insert a new bin
    } else {
      index = std::abs(index) - 1;
      bins_.insert(bins_.begin() + index, bin);
    }
  }

  /**
   * Merge n + 1 number of bins to n bins based on update algorithm
   */
  void MergeTwoBinsWithMinGap() {
    int min_gap_idx = -1;
    double min_gap = DBL_MAX;
    for (uint8_t i = 0; i < bins_.size() - 1; i++) {
      double gap = std::abs(bins_[i].GetPoint() - bins_[i + 1].GetPoint());
      if (gap < min_gap) {
        min_gap = gap;
        min_gap_idx = i;
      }
    }
    // NOISEPAGE_ASSERT(min_gap_idx >= 0 && min_gap_idx < bins_.size());
    Bin &prev_bin = bins_[min_gap_idx];
    Bin &next_bin = bins_[min_gap_idx + 1];
    prev_bin.MergeWith(next_bin);
    bins_.erase(bins_.begin() + min_gap_idx + 1);
  }

  /**
   * A more useful binary search based on Java's Collections.binarySearch.
   * It returns either the found index or -1 if not found,
   * where index is position for insertion (i.e., the first index with
   * element **greater** than the key).
   * @tparam X search type
   * @param vec the vector to perform the binary search on
   * @param start the starting offset
   * @param end the end offset
   * @param key the search key
   * @return found offset or -1 if not found
   */
  template <typename X>
  int BinarySearch(const std::vector<X> &vec, int start, int end, const X &key) {
    if (start > end) {
      return -(start + 1);
    }

    const int middle = start + ((end - start) / 2);

    if (vec[middle] == key) {
      return middle;
    }
    if (vec[middle] > key) {
      return BinarySearch(vec, start, middle - 1, key);
    }
    return BinarySearch(vec, middle + 1, end, key);
  }

  /**
   * Compute the interval between two bins
   * @param bins
   * @param i
   * @return
   */
  std::tuple<double, double, double, double> GetInterval(std::vector<Bin> bins, uint32_t i) {
    NOISEPAGE_ASSERT(i < bins.size() - 1, "Requested interval is greater than max # of bins");
    return std::make_tuple(bins[i].GetPoint(), bins[i + 1].GetPoint(), bins[i].GetCount(), bins[i + 1].GetCount());
  }
};

}  // namespace noisepage::optimizer
