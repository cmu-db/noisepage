#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/parameter.h"
#include "parser/update_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace terrier::planner {

using SetClause = std::pair<catalog::col_oid_t, common::ManagedPointer<parser::AbstractExpression>>;

/**
 * Plan node for update
 */
class UpdatePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an delete plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder)

    /**
     * @param table_oid the OID of the target SQL table
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * @param indexed_update whether to update indexes
     * @return builder object
     */
    Builder &SetIndexedUpdate(bool indexed_update) {
      indexed_update_ = true;
      return *this;
    }

    /**
     * @param clause SET clause in a update statement
     * @return builder object
     */
    Builder &AddSetClause(SetClause clause) {
      sets_.push_back(clause);
      return *this;
    }

    /**
     * Build the delete plan node
     * @return plan node
     */
    std::unique_ptr<UpdatePlanNode> Build() {
      return std::unique_ptr<UpdatePlanNode>(new UpdatePlanNode(std::move(children_), std::move(output_schema_),
                                                                table_oid_, indexed_update_, std::move(sets_)));
    }

   protected:
    /**
     * OID of the table to update
     */
    catalog::table_oid_t table_oid_;

    /**
     * Whether indexes are updated.
     */
    bool indexed_update_;

    /**
     * Set Clauses
     */
    std::vector<SetClause> sets_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param table_oid OID of the target SQL table
   * @param indexed_upate whether to update indexes
   * @param sets SET clauses
   */
  UpdatePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
                 catalog::table_oid_t table_oid, bool indexed_update, std::vector<SetClause> sets)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        table_oid_(table_oid),
        indexed_update_(indexed_update),
        sets_(std::move(sets)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  UpdatePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(UpdatePlanNode)

  /**
   * @return the OID of the target table to operate on
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return whether to update indexes
   */
  bool GetIndexedUpdate() const { return indexed_update_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UPDATE; }

  /**
   * @return SET clauses
   */
  const std::vector<SetClause> &GetSetClauses() const { return sets_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the table to update
   */
  catalog::table_oid_t table_oid_;

  /**
   * Whether to update indexes
   */
  bool indexed_update_;

  /**
   * SET clauses
   */
  std::vector<SetClause> sets_;
};

DEFINE_JSON_DECLARATIONS(UpdatePlanNode);

}  // namespace terrier::planner
