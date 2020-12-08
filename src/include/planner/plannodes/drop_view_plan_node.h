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
 *  The plan node for dropping views
 */
class DropViewPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop view plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_oid the OID of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param view_oid the OID of the view to drop
     * @return builder object
     */
    Builder &SetViewOid(catalog::view_oid_t view_oid) {
      view_oid_ = view_oid;
      return *this;
    }

    /**
     * @param if_exists true if "IF EXISTS" was used
     * @return builder object
     */
    Builder &SetIfExist(bool if_exists) {
      if_exists_ = if_exists;
      return *this;
    }

    /**
     * Build the drop view plan node
     * @return plan node
     */
    std::unique_ptr<DropViewPlanNode> Build();

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of the view to drop
     */
    catalog::view_oid_t view_oid_;

    /**
     * Whether "IF EXISTS" was used
     */
    bool if_exists_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param view_oid OID of the view to drop
   * @param plan_node_id Plan node id
   */
  DropViewPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                   std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                   catalog::view_oid_t view_oid, bool if_exists, plan_node_id_t plan_node_id);

 public:
  /**
   * Default constructor for deserialization
   */
  DropViewPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropViewPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_VIEW; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the view to drop
   */
  catalog::view_oid_t GetViewOid() const { return view_oid_; }

  /**
   * @return true if "IF EXISTS" was used
   */
  bool IsIfExists() const { return if_exists_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of the view to drop
   */
  catalog::view_oid_t view_oid_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DropViewPlanNode);

}  // namespace noisepage::planner
