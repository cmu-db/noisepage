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
 protected:
  /**
   * Builder for a delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param target_table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTargetTableOid(catalog::table_oid_t target_table_oid) {
      target_table_oid_ = target_table_oid;
      return *this;
    }

    /**
     * @param table_name name of the target table
     * @return builder object
     */
    Builder &SetTableName(std::string table_name) {
      table_name_ = std::move(table_name);
      return *this;
    }

    /**
     * @param delete_condition expression of delete condition
     * @return builder object
     */
    Builder &SetDeleteCondition(std::shared_ptr<parser::AbstractExpression> delete_condition) {
      delete_condition_ = std::move(delete_condition);
      return *this;
    }

    /**
     * @param delete_stmt the SQL DELETE statement
     * @return builder object
     */
    Builder &SetFromDeleteStatement(parser::DeleteStatement *delete_stmt) {
      table_name_ = delete_stmt->GetDeletionTable()->GetTableName();
      delete_condition_ = delete_stmt->GetDeleteCondition();
      // TODO(Gus,Wen) get table OID from catalog
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::shared_ptr<DeletePlanNode> Build() {
      return std::shared_ptr<DeletePlanNode>(new DeletePlanNode(std::move(children_), std::move(output_schema_),
                                                                estimated_cardinality_, target_table_oid_,
                                                                std::move(table_name_), std::move(delete_condition_)));
    }

   protected:
    catalog::table_oid_t target_table_oid_;
    std::string table_name_;
    std::shared_ptr<parser::AbstractExpression> delete_condition_;
  };

  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param estimated_cardinality estimated cardinality of output of node
   * @param target_table_oid the OID of the target SQL table
   * @param table_name name of the target table
   * @param delete_condition expression of delete condition
   */
  DeletePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::shared_ptr<OutputSchema> output_schema,
                 uint32_t estimated_cardinality, catalog::table_oid_t target_table_oid, std::string table_name,
                 std::shared_ptr<parser::AbstractExpression> delete_condition)
      : AbstractPlanNode(std::move(children), std::move(output_schema), estimated_cardinality),
        target_table_oid_(target_table_oid),
        table_name_(std::move(table_name)),
        delete_condition_(std::move(delete_condition)) {}

 public:
  DeletePlanNode() = delete;

  /**
   * @return the OID of the table to be deleted
   */
  catalog::table_oid_t GetTargetTableOid() const { return target_table_oid_; }

  /**
   * @return the names of the target table
   */
  const std::string &GetTableName() const { return table_name_; }

  /**
   * @return the expression of delete condition
   */
  std::shared_ptr<parser::AbstractExpression> GetDeleteCondition() const { return delete_condition_; }

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
