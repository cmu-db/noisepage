#pragma once

#include <memory>
#include <string>
#include "parser/delete_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {

namespace plan_node {
/**
 * The plan node for DELETE
 */
class DeletePlanNode : public AbstractPlanNode {
 public:
  DeletePlanNode() = delete;

  /**
   * Instantiate a DeletePlan Node
   * @param target_table_oid the table to be deleted
   */
  explicit DeletePlanNode(catalog::table_oid_t target_table_oid);

  /**
   * Instantiate new DeletePlanNode
   * @param delete_stmt the DELETE statement from parser
   */
  explicit DeletePlanNode(parser::DeleteStatement *delete_stmt);

  /**
   * @return the OID of the table to be deleted
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  catalog::table_oid_t target_table_oid_;                         // the table to be deleted
  std::string table_name_;                                        // name of the table
  std::shared_ptr<parser::AbstractExpression> delete_condition_;  // expression of delete condition

 public:
  DISALLOW_COPY_AND_MOVE(DeletePlanNode);
};

}  // namespace plan_node
}  // namespace terrier
