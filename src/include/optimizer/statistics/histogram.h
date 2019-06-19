#pragma once

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <vector>
#include <tuple>

#include "common/macros.h"
#include "loggers/optimizer_logger.h"
#include "type/type_id.h"

namespace terrier::optimizer {

/**
 * Online histogram implementation based on this paper (referred to below as JMLR10):
 *    A Streaming Parallel Decision Tree Algorithm
 *    http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * Specifically Algorithm 1, 3, and 4.
 */
template <typename PointType>
class Histogram {
 public:

  /**
   * Constructor.
   * @param max_bins maximum number of bins in histogram.
   */
  Histogram(const uint8_t max_bins)
      : max_bins_{max_bins},
        bins_{},
        total_{0},
        minimum_{DBL_MAX},
        maximum_{DBL_MIN} {}

  /**
   *
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
    double IncCount(double delta) {
      count_ += delta;
      return count_;
    }

    bool operator<(const Bin &bin) const { return point_ < bin.point_; }

    bool operator==(const Bin &bin) const { return point_ == bin.point_; }

    bool operator>(const Bin &bin) const { return point_ > bin.point_; }

    bool operator!=(const Bin &bin) const { return point_ != bin.point_; }

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
   * @param value the point to update
   */
  void Update(const PointType &value) {
    double point = static_cast<double>(value);
    Bin bin{point, 1};
    InsertBin(bin);
    if (bins_.size() > max_bins_) {
      MergeTwoBinsWithMinGap();
    }
  }

  /**
   * For the given value point (where p1 < b < pB), return an estimate
   * of the number of points in the interval [-Inf, b]
   * @param point the value point to estimate
   * @return the estimate of the # of points
   */
  double Sum(double point) {
    if (bins_.size() == 0) return 0.0;

    if (point >= bins_.back().GetPoint()) {
      return total_;
    } else if (point < bins_.front().GetPoint()) {
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
    TERRIER_ASSERT(max_bins_ > 0, "# of max bins is less than one");

    std::vector<double> res{};
    if (bins_.size() <= 1 || total_ <= 0) return res;

    uint32_t i = 0;
    for (uint32_t j = 0; j < bins_.size() - 1; j++) {
      double s = (j * 1.0 + 1.0) / max_bins_ * total_;
      while (i < bins_.size() - 1 && Sum(bins_[i + 1].GetPoint()) < s) {
        i += 1;
      }
      TERRIER_ASSERT(i < bins_.size() - 1, "Invalid bin offset");
      double point_i, point_i1, count_i, count_i1;
      std::tie(point_i, point_i1, count_i, count_i1) = GetInterval(bins_, i);

      double d = s - Sum(bins_[i].GetPoint());
      double a = count_i1 - count_i;
      double b = 2.0 * count_i;
      double c = -2.0 * d;
      double z =
          a != 0 ? (-b + std::sqrt(b * b - 4.0 * a * c)) / (2.0 * a) : -c / b;
      double uj = point_i + (point_i1 - point_i) * z;
      res.push_back(uj);
    }
    return res;
  }

  /**
   *
   * @return
   */
  double GetMaxValue() const { return maximum_; }

  /**
   *
   * @return
   */
  double GetMinValue() const { return minimum_; }

  /**
   *
   * @return
   */
  double GetTotalValueCount() const { return std::floor(total_); }

  /**
   *
   * @return
   */
  uint8_t GetMaxBinSize() const { return max_bins_; }

 private:

  /**
   *
   */
  const uint8_t max_bins_;

  /**
   *
   */
  std::vector<Bin> bins_;

  /**
   *
   */
  double total_;

  /**
   *
   */
  double minimum_;

  /**
   *
   */
  double maximum_;

  /**
   *
   * @param bin
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

    if (index >= 0) {
      bins_[index].IncCount(bin.GetCount());
    } else {
      index = std::abs(index) - 1;
      bins_.insert(bins_.begin() + index, bin);
    }
  }

  /* Merge n + 1 number of bins to n bins based on update algorithm. */
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
    //		TERRIER_ASSERT(min_gap_idx >= 0 && min_gap_idx < bins_.size());
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
  int BinarySearch(const std::vector<X> &vec, int start, int end,
                   const X &key) {
    if (start > end) {
      return -(start + 1);
    }

    const int middle = start + ((end - start) / 2);

    if (vec[middle] == key) {
      return middle;
    } else if (vec[middle] > key) {
      return BinarySearch(vec, start, middle - 1, key);
    }
    return BinarySearch(vec, middle + 1, end, key);
  }

  /**
   *
   * @param bins
   * @param i
   * @return
   */
  std::tuple<double, double, double, double> GetInterval(
      std::vector<Bin> bins, uint32_t i) {
    TERRIER_ASSERT(i < bins.size() - 1, "Requested interval is greater than max # of bins");
    return std::make_tuple(bins[i].GetPoint(), bins[i + 1].GetPoint(), bins[i].GetCount(), bins[i + 1].GetCount());
  }

  friend std::ostream &operator<<(std::ostream &os, const Histogram<PointType> &h) {
    os << "Histogram: "
        << "total=[" << h.total_ << "] "
        << "num_bins=[" << h.bins_.size() << "]"
        << std::endl;
    for (Bin b : h.bins_) {
      os << "  " << b << std::endl;
    }
    return os;
  }

  void PrintUniform(const std::vector<double> &vec) {
    std::string output{"{"};
    for (uint8_t i = 0; i < vec.size(); i++) {
      output += std::to_string(vec[i]);
      output += (i == vec.size() - 1 ? "}" : ", ");
    }
    OPTIMIZER_LOG_INFO("%s", output.c_str());
  }

};

}  // namespace terrier::optimizer