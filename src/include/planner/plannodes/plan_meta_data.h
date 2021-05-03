#pragma once

#include <unordered_map>
#include <utility>

#include "planner/plannodes/plan_node_defs.h"

namespace noisepage::planner {

/**
 * Meta data for a plan tree
 */
class PlanMetaData {
 public:
  /**
   * Meta data for every plan node
   */
  class PlanNodeMetaData {
   public:
    PlanNodeMetaData() = default;

    /**
     * Construct a PlanNodeMetaData with a cardinality value
     * @param cardinality output cardinality of the plan node
     * @param table_num_rows number of rows in the base table (for sequential or index scans)
     * @param filter_column_selectivities maps from column id to the selectivity on that column (multiplied
     *   with duplicates)
     */
    explicit PlanNodeMetaData(size_t cardinality, size_t table_num_rows,
                              std::unordered_map<catalog::col_oid_t, double> filter_column_selectivities)
        : cardinality_(cardinality),
          table_num_rows_(table_num_rows),
          filter_column_selectivities_(std::move(filter_column_selectivities)) {}

    /**
     * @return the output cardinality
     */
    size_t GetCardinality() const { return cardinality_; }

    /**
     * @return the number of rows in the table to scan
     */
    size_t GetTableNumRows() const { return table_num_rows_; }

    /**
     * @return the number of rows in the table to scan
     */
    double GetFilterColumnSelectivity(catalog::col_oid_t col_oid) const {
      if (filter_column_selectivities_.find(col_oid) == filter_column_selectivities_.end())
        // Does not filter on this column
        return 1;

      return filter_column_selectivities_.at(col_oid);
    }

   private:
    size_t cardinality_;
    size_t table_num_rows_;
    std::unordered_map<catalog::col_oid_t, double> filter_column_selectivities_;
  };

  /**
   * Add meta data for a plan node
   * @param plan_node_id plan node id
   * @param meta_data plan node meta data
   */
  void AddPlanNodeMetaData(plan_node_id_t plan_node_id, const PlanNodeMetaData &meta_data) {
    NOISEPAGE_ASSERT(plan_node_meta_data_.count(plan_node_id) == 0, "already exists meta data for the plan node");
    plan_node_meta_data_[plan_node_id] = meta_data;
  }

  /**
   * Get the meta data for a plan node
   * @param plan_node_id plan node id
   * @return plan node meta data
   */
  const PlanNodeMetaData &GetPlanNodeMetaData(plan_node_id_t plan_node_id) {
    NOISEPAGE_ASSERT(plan_node_meta_data_.count(plan_node_id) != 0, "there is no meta data for the plan node");
    return plan_node_meta_data_[plan_node_id];
  }

 private:
  /**
   * Meta data for all plan nodes
   */
  std::unordered_map<plan_node_id_t, PlanNodeMetaData> plan_node_meta_data_;
};
}  // namespace noisepage::planner
