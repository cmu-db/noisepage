#pragma once

#include <memory>
#include <vector>

#include "parser/drop_statement.h"
#include "parser/parser_defs.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for dropping user-defined functions.
 */
class DropFunctionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an create function plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid The OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param proc_oid The OID of the procedure
     * @return builder object
     */
    Builder &SetProcedureOid(catalog::proc_oid_t proc_oid) {
      proc_oid_ = proc_oid;
      return *this;
    }

    /**
     * Build the drop function plan node
     * @return plan node
     */
    std::unique_ptr<DropFunctionPlanNode> Build();

   protected:
    /** OID of the database */
    catalog::db_oid_t database_oid_;
    /** OID of the procedure */
    catalog::proc_oid_t proc_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param proc_oid OID of the procedure
   * @param plan_node_id Plan node ID
   */
  DropFunctionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                       catalog::proc_oid_t proc_oid, plan_node_id_t plan_node_id);

 public:
  /** Default constructor used for deserialization */
  DropFunctionPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropFunctionPlanNode)

  /** @return the type of this plan node */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_FUNC; }

  /** @return OID of the database */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /** @return OID of the procedure */
  catalog::proc_oid_t GetProcedureOid() const { return proc_oid_; }

  /** @return the hashed value of this plan node */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  /** Serialize to JSON representation */
  nlohmann::json ToJson() const override;
  /** Deserialize from JSON representation */
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /** OID of database */
  catalog::db_oid_t database_oid_;
  /** OID of procedure */
  catalog::proc_oid_t proc_oid_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DropFunctionPlanNode);

}  // namespace noisepage::planner
