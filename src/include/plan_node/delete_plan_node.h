#pragma once

#include <memory>
#include <string>
#include "parser/delete_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier {

namespace storage {
class SqlTable;
}

namespace plan_node {
/**
 * The plan node for DELETE
 */
class DeletePlanNode : public AbstractPlanNode {
 public:
  DeletePlanNode() = delete;

  /**
   * Instantiate a DeletePlan Node
   * @param target_table the table to be deleted
   */
  explicit DeletePlanNode(std::shared_ptr<storage::SqlTable> target_table);

  /**
   * Instantiate new DeletePlanNode
   * @param delete_stmt the DELETE statement from parser
   */
  explicit DeletePlanNode(parser::DeleteStatement *delete_stmt);

  /**
   * @return the table to be deleted
   */
  std::shared_ptr<storage::SqlTable> GetTargetTable() const { return target_table_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  /**
   * @return debug info
   */
  const std::string GetInfo() const override { return "DeletePlanNode"; }

  /**
   * @return pointer to a copy of delete plan
   */
  std::unique_ptr<AbstractPlanNode> Copy() const override {
    return std::unique_ptr<AbstractPlanNode>(new DeletePlanNode(target_table_));
  }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;
  bool operator!=(const AbstractPlanNode &rhs) const override { return !(*this == rhs); }

 private:
  std::shared_ptr<storage::SqlTable> target_table_ = nullptr;     // the table to be deleted
  std::string table_name_;                                        // name of the table
  std::shared_ptr<parser::AbstractExpression> delete_condition_;  // expression of delete condition

  DISALLOW_COPY_AND_MOVE(DeletePlanNode);
};

}  // namespace plan_node
}  // namespace terrier
