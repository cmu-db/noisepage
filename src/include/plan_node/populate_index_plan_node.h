
#pragma once

#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog_defs.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {
namespace plan_node {

/**
 * Plan node for populating an index
 * IMPORTANT All tiles got from child must have the same physical schema.
 */

class PopulateIndexPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a PopulateIndexPlanNode
   * @param target_table_oid the table the index is created for
   * @param column_ids the column IDs to be populated into the index
   */
  explicit PopulateIndexPlanNode(catalog::table_oid_t target_table_oid, std::vector<catalog::col_oid_t> column_ids);

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::POPULATE_INDEX; }

  /**
   * @return the column IDs to be populated into the index
   */
  const std::vector<catalog::col_oid_t> &GetColumnIds() const { return column_ids_; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "PopulateIndexPlanNode"; }

  /**
   * @return the OID of the Ftarget table
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

 private:
  // Target table OID
  catalog::table_oid_t target_table_oid_;
  // Column Ids
  std::vector<catalog::col_oid_t> column_ids_;

  DISALLOW_COPY_AND_MOVE(PopulateIndexPlanNode);
};

}  // namespace plan_node
}  // namespace terrier
