#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/delete_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {
/**
 * The plan node for DELETE
 */
class DeletePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::unique_ptr<DeletePlanNode> Build() {
      return std::unique_ptr<DeletePlanNode>(
          new DeletePlanNode(std::move(children_), std::move(output_schema_), table_oid_));
    }

   protected:
    /**
     * the table to be deleted
     */
    catalog::table_oid_t table_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param table_oid the OID of the target SQL table
   */
  DeletePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                 catalog::table_oid_t table_oid)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), table_oid_(table_oid) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  DeletePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DeletePlanNode)

  /**
   * @return OID of the table to be deleted
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /** @return the type of this plan node */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DELETE; }

  /** @return the hashed value of this plan node */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Table to be deleted
   */
  catalog::table_oid_t table_oid_;
};

DEFINE_JSON_DECLARATIONS(DeletePlanNode);

}  // namespace terrier::planner
