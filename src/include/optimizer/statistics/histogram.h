#pragma once

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/json.h"
#include "common/macros.h"
#include "execution/sql/value.h"
#include "loggers/optimizer_logger.h"
#include "type/type_id.h"

namespace noisepage::optimizer {

/**
 * Online histogram implementation based on this paper (referred to below as JMLR10):
 *    A Streaming Parallel Decision Tree Algorithm
 *    http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * Specifically Algorithm 1, 3, and 4.
 *
 * Histogram supports any key type that can be converted into a double. However in order to get meaningful results from
 * the histogram the ordering of the keys should be preserved in this conversion. So for example if key1 > key2
 * then ConvertToPoint(key1) > ConvertToPoint(key2) where ConvertToPoint(k) converts k to a double.
 */
template <typename KeyType>
class Histogram {
  static constexpr uint8_t DEFAULT_MAX_BINS = 64;

 public:
  /**
   * Constructor defaults to DEFAULT_MAX_BINS for bin size
   */
  Histogram() : max_bins_(DEFAULT_MAX_BINS), bins_{}, total_{0}, minimum_{DBL_MAX}, maximum_{DBL_MIN} {}
  /**
   * Constructor.
   * @param max_bins maximum number of bins in histogram.
   */
  explicit Histogram(const uint8_t max_bins)
      : max_bins_{max_bins}, bins_{}, total_{0}, minimum_{DBL_MAX}, maximum_{DBL_MIN} {}

  /**
   * Copy constructor
   * @param other Histogram to copy
   */
  Histogram(const Histogram &other) = default;

  /**
   * Move constructor
   * @param other Histogram to copy
   */
  Histogram(Histogram &&other) noexcept = default;

  /**
   * Copy assignment operator
   * @param other Histogram to copy
   * @return
   */
  Histogram &operator=(const Histogram &other) = default;

  /**
   * Move assignment operator
   * @param other Histogram to copy
   * @return
   */
  Histogram &operator=(Histogram &&other) noexcept = default;

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
     * Copy Constructor
     * @param other Bin to copy
     */
    Bin(const Bin &other) = default;

    /**
     * Move Constructor
     * @param other Bin to copy
     */
    Bin(Bin &&other) noexcept = default;

    /**
     * Copy assignment operator
     * @param other Bin to copy
     * @return this after copying
     */
    Bin &operator=(const Bin &other) = default;

    /**
     * Move assignment operator
     * @param other Bin to copy
     * @return this after copying
     */
    Bin &operator=(Bin &&other) noexcept = default;

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
     * Convert Bin to json
     * @return json representation of a Bin
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["point"] = point_;
      j["count"] = count_;
      return j;
    }

    /**
     * Convert json to Bin
     * @param j json representation of a Bin
     * @return Bin object parsed from json
     */
    static Bin FromJson(const nlohmann::json &j) {
      auto point = j.at("point").get<double>();
      auto count = j.at("count").get<double>();
      return Bin(point, count);
    }

    /**
     * Serialize Bin object into byte array
     * @param[out] size length of byte array
     * @return byte array representation of Bin
     */
    std::unique_ptr<byte[]> Serialize(size_t *size) const {
      const std::string json_str = ToJson().dump();
      *size = json_str.size();
      auto buffer = std::make_unique<byte[]>(*size);
      std::memcpy(buffer.get(), json_str.c_str(), *size);
      return buffer;
    }

    /**
     * Deserialize Bin object from byte array
     * @param buffer byte array representation of Bin
     * @param size length of byte array
     * @return Deserialized Bin object
     */
    static Bin Deserialize(const byte *buffer, size_t size) {
      std::string json_str(reinterpret_cast<const char *>(buffer), size);
      auto json = nlohmann::json::parse(json_str);
      return Bin::FromJson(json);
    }

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
    auto point = ConvertToPoint(key);
    Bin bin{point, 1};
    InsertBin(bin);
    if (bins_.size() > max_bins_) {
      MergeTwoBinsWithMinGap();
    }
  }

  /**
   * For the given key key (where p1 < b < pB), return an estimate
   * of the number of points in the interval [-Inf, b]
   * @param key the value key to estimate
   * @return the estimate of the # of points
   */
  double EstimateItemCount(KeyType key) {
    double point = ConvertToPoint(key);

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
   * Merge Histogram with another Histogram
   * @param histogram Histogram to merge with this
   */
  void Merge(const Histogram<KeyType> &histogram) {
    for (auto &bin : histogram.bins_) {
      InsertBin(bin);
    }
  }

  /**
   * Clear the Histogram object
   */
  void Clear() {
    bins_.clear();
    total_ = 0;
    minimum_ = DBL_MAX;
    maximum_ = DBL_MIN;
  }

  /**
   * @return the largest value that we have in this histogram
   */
  double GetMaxValue() const { return maximum_; }

  /**
   * Compares a value to the max value in the histogram
   * @param value value to compare to max
   * @return true if value is greater than or equal to max, false otherwise
   */
  bool IsGreaterThanOrEqualToMaxValue(KeyType value) const { return ConvertToPoint(value) >= maximum_; }

  /**
   * @return the smallest value that we have in this histogram
   */
  double GetMinValue() const { return minimum_; }

  /**
   * Compares a value to the min value in the histogram
   * @param value value to compare to min
   * @return true if value is less than min, false otherwise
   */
  bool IsLessThanMinValue(KeyType value) const { return ConvertToPoint(value) < minimum_; }

  /**
   * @return the total number of values that are recorded in this histogram
   */
  double GetTotalValueCount() const { return std::floor(total_); }

  /**
   * @return the maximum number of bins that this histogram supports
   */
  uint8_t GetMaxBinSize() const { return max_bins_; }

  /**
   * Convert Histogram to json
   * @return json representation of a Histogram
   */
  nlohmann::json ToJson() const {
    nlohmann::json j;
    j["max_bins"] = max_bins_;
    std::vector<nlohmann::json> bins_json;
    for (const auto &bin : bins_) {
      bins_json.template emplace_back(bin.ToJson());
    }
    j["bins"] = bins_json;
    j["total"] = total_;
    j["minimum"] = minimum_;
    j["maximum"] = maximum_;
    return j;
  }

  /**
   * Convert json to Histogram
   * @param j json representation of a Histogram
   * @return Histogram object parsed from json
   */
  static Histogram FromJson(const nlohmann::json &j) {
    auto max_bins = j.at("max_bins").get<const uint8_t>();
    Histogram histogram(max_bins);
    auto bin_jsons = j.at("bins").get<std::vector<nlohmann::json>>();
    for (const auto &bin_json : bin_jsons) {
      histogram.bins_.emplace_back(Bin::FromJson(bin_json));
    }
    histogram.total_ = j.at("total").get<double>();
    histogram.minimum_ = j.at("minimum").get<double>();
    histogram.maximum_ = j.at("maximum").get<double>();
    return histogram;
  }

  /**
   * Serialize Histogram object into byte array
   * @param[out] size length of byte array
   * @return byte array representation of Histogram
   */
  std::unique_ptr<byte[]> Serialize(size_t *size) const {
    const std::string json_str = ToJson().dump();
    *size = json_str.size();
    auto buffer = std::make_unique<byte[]>(*size);
    std::memcpy(buffer.get(), json_str.c_str(), *size);
    return buffer;
  }

  /**
   * Deserialize Histogram object from byte array
   * @param buffer byte array representation of Histogram
   * @param size length of byte array
   * @return Deserialized Histogram object
   */
  static Histogram Deserialize(const byte *buffer, size_t size) {
    std::string json_str(reinterpret_cast<const char *>(buffer), size);
    auto json = nlohmann::json::parse(json_str);
    return Histogram::FromJson(json);
  }

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
    for (const Bin &b : h.bins_) {
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
   * The total number of values that are recorded in this histogram
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

  /*
   * We need a way to convert the SQL types to doubles to work with the Histogram.
   */
  static double ConvertToPoint(const decltype(execution::sql::BoolVal::val_) &key) { return static_cast<double>(key); }

  static double ConvertToPoint(const decltype(execution::sql::Integer::val_) &key) { return static_cast<double>(key); }

  static double ConvertToPoint(const int &key) { return static_cast<double>(key); }

  static double ConvertToPoint(const decltype(execution::sql::Real::val_) &key) { return static_cast<double>(key); }

  static double ConvertToPoint(const decltype(execution::sql::DecimalVal::val_) &key) {
    return static_cast<double>(key);
  }

  /*
   * Inspired by:
   * https://stackoverflow.com/questions/1914977/map-strings-to-numbers-maintaining-the-lexicographic-ordering/1915258#1915258
   * Which attempts to map the string to a number between 0 and 1 while maintaining lexicographical order.
   * An extension was made from that SO post to support characters beyond a-z and one issue was corrected
   */
  static double ConvertToPoint(const decltype(execution::sql::StringVal::val_) &key) {
    double result = 0;
    double scale = 1;
    auto s = key.StringView();
    for (size_t i = 0; i < key.Size(); i++) {
      scale /= 256;
      int index = 1 + s.at(i);
      result += index * scale;
    }
    return result;
  }

  static double ConvertToPoint(const decltype(execution::sql::DateVal::val_) &key) {
    return static_cast<double>(key.ToNative());
  }

  static double ConvertToPoint(const decltype(execution::sql::TimestampVal::val_) &key) {
    return static_cast<double>(key.ToNative());
  }
};

}  // namespace noisepage::optimizer
