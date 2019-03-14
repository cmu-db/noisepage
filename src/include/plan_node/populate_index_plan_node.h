
#pragma once

#include "catalog/catalog_defs.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {

namespace storage {
class SqlTable;
}

namespace plan_node {

/**
 * Plan node for populating an index
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class PopulateIndexPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a PopulateIndexPlanNode
   * @param target_table the table the index is created for
   * @param column_ids the column IDs to be populated into the index
   */
  explicit PopulateIndexPlanNode(std::shared_ptr<storage::SqlTable> target_table,
                                 std::vector<catalog::col_oid_t> column_ids);

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const override { return PlanNodeType::POPULATE_INDEX; }

  /**
   * @return the column IDs to be populated into the index
   */
  inline const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_ids_; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const { return "Populate Index Plan Node"; }

  /**
   * @return the target table
   */
  std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new PopulateIndexPlanNode(target_table_, column_ids_));
  }

 private:
  // Target table
  std::shared_ptr<storage::SqlTable> target_table_ = nullptr;
  // Column Ids
  std::vector<catalog::col_oid_t> column_ids_;

  DISALLOW_COPY_AND_MOVE(PopulateIndexPlanNode);
};

}  // namespace plan_node
}  // namespace terrier
