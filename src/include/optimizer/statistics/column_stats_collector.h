#pragma once

#include <vector>
#include <functional>
#include <libcount/hll.h>

#include "optimizer/statistics/count_min_sketch.h"
//#include "optimizer/statistics/top_k_elements.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/hyperloglog.h"

#include "type/transient_value.h"

namespace terrier {
namespace optimizer {

/**
 *
 */
class ColumnStatsCollector {
 public:
  using ValueFrequencyPair = std::pair<type::TransientValue, double>;

  /**
   * Constructor
   * @param database_id
   * @param table_id
   * @param column_id
   * @param column_type
   * @param column_name
   */
  ColumnStatsCollector(catalog::db_oid_t database_id, catalog::table_oid_t table_id, catalog::col_oid_t column_id,
                       type::TypeId column_type, std::string column_name);

  /**
   * Destructor
   */
  ~ColumnStatsCollector();

  /**
   *
   * @param value
   */
  void AddValue(const type::TransientValue& value);

  double GetFracNull();

  inline std::vector<ValueFrequencyPair> GetCommonValueAndFrequency() {
    //return topk_.GetAllOrderedMaxFirst();
  }

  inline uint64_t GetCardinality() { return hll_.EstimateCardinality(); }

  inline double GetCardinalityError() { return hll_.RelativeError(); }

  inline std::vector<double> GetHistogramBound() { return hist_.Uniform(); }

  inline std::string GetColumnName() { return column_name_; }

  inline void SetColumnIndexed() { has_index_ = true; }

  inline bool HasIndex() { return has_index_; }

  inline int GetHllPrecision() { return hll_precision_; }

  inline uint64_t GetSketchWidth() { return sketch_width_; }

  inline uint8_t GetMaxBins() { return max_bins_; }

  inline uint8_t GetTopK() { return top_k_; }

 private:
  const catalog::db_oid_t database_id_;
  const catalog::table_oid_t table_id_;
  const catalog::col_oid_t column_id_;
  const type::TypeId column_type_;
  const std::string column_name_;
  HyperLogLog<type::TransientValue> hll_;
  Histogram<type::TransientValue> hist_;
  CountMinSketch<type::TransientValue> sketch_;
  //TopKElements topk_;

  bool has_index_ = false;

  size_t null_count_ = 0;
  size_t total_count_ = 0;

  /* Default parameters for probabilistic stats collector */
  int hll_precision_ = 8;
  uint64_t sketch_width_ = 100;
  uint8_t max_bins_ = 100;
  uint8_t top_k_ = 10;

  ColumnStatsCollector(const ColumnStatsCollector&);
  void operator=(const ColumnStatsCollector&);
};

}  // namespace optimizer
}  // namespace terrier