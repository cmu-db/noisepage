#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/drop_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {
/**
 *  The plan node for dropping tables
 */
class DropTablePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop table plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param table_oid the OID of the table to drop
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

    /**
     * Build the drop table plan node
     * @return plan node
     */
    std::unique_ptr<DropTablePlanNode> Build();

   protected:
    /**
     * OID of the table to drop
     */
    catalog::table_oid_t table_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table to drop
   * @param plan_node_id Plan node id
   */
  DropTablePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                    std::unique_ptr<OutputSchema> output_schema, catalog::table_oid_t table_oid,
                    plan_node_id_t plan_node_id);

 public:
  /**
   * Default constructor for deserialization
   */
  DropTablePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropTablePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_TABLE; }

  /**
   * @return OID of the table to drop
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  catalog::table_oid_t table_oid_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DropTablePlanNode);

}  // namespace noisepage::planner
