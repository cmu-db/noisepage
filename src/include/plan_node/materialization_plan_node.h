#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "output_schema.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

class MaterializationPlanNode : public AbstractPlanNode {
 public:
  /**
   * Instantiate a MaterializationPlanNode
   * @param old_to_new_cols mapping from input to output columns
   * @param schema  output schema
   * @param physify_flag indicate whether to create a physical tile
   */
  MaterializationPlanNode(const std::unordered_map<catalog::col_oid_t, catalog::col_oid_t> &old_to_new_cols,
                          std::shared_ptr<OutputSchema> output_schema, bool physify_flag)
      : AbstractPlanNode(output_schema), old_to_new_cols_(old_to_new_cols), physify_flag_(physify_flag) {}

  /**
   * Instantiate a MaterializationPlanNode
   * @param physify_flag indicate whether to create a physical tile
   */
  MaterializationPlanNode(bool physify_flag) : AbstractPlanNode(nullptr), physify_flag_(physify_flag) {}

  /**
   * @return input to output column mapping
   */
  inline const std::unordered_map<catalog::col_oid_t, catalog::col_oid_t> &GetOldToNewCols() const {
    return old_to_new_cols_;
  }

  /**
   * @return physify flag
   */
  inline bool GetPhysifyFlag() const { return physify_flag_; }

  /**
   * @return the type of this plan node
   */
  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::MATERIALIZE; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const { return "Materialize Plan Node"; }

  /**
   * @return a unique pointer to a copy of this plan node
   */
  std::unique_ptr<AbstractPlanNode> Copy() const {
    return std::unique_ptr<AbstractPlanNode>(
        new MaterializationPlanNode(old_to_new_cols_, GetOutputSchema(), physify_flag_));
  }

 private:
  /**
   * Mapping of old column ids to new column ids after materialization.
   */
  // TODO(Gus,Wen) This most likely needs to be offset OID, which is not available in catalog
  std::unordered_map<catalog::col_oid_t, catalog::col_oid_t> old_to_new_cols_;

  /**
   * Whether to create a physical tile or just pass through underlying
   * logical tile
   */
  bool physify_flag_;

 private:
  DISALLOW_COPY_AND_MOVE(MaterializationPlanNode);
};

}  // namespace terrier::plan_node