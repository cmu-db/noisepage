
#pragma once

#include <memory>
#include <string>
#include <vector>
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
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::POPULATE_INDEX; }

  /**
   * @return the column IDs to be populated into the index
   */
  const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_ids_; }

  /**
   * @return the target table
   */
  std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

 private:
  // Target table
  std::shared_ptr<storage::SqlTable> target_table_ = nullptr;
  // Column Ids
  std::vector<catalog::col_oid_t> column_ids_;

 public:
  DISALLOW_COPY_AND_MOVE(PopulateIndexPlanNode);
};

}  // namespace plan_node
}  // namespace terrier
