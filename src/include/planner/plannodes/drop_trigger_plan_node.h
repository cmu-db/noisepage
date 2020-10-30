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
 *  The plan node for dropping triggers
 */
class DropTriggerPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop trigger plan node
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
     * @param namespace_oid OID of the namespace
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * @param trigger_oid the OID of the trigger to drop
     * @return builder object
     */
    Builder &SetTriggerOid(catalog::trigger_oid_t trigger_oid) {
      trigger_oid_ = trigger_oid;
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
     * Build the drop trigger plan node
     * @return plan node
     */
    std::unique_ptr<DropTriggerPlanNode> Build() {
      return std::unique_ptr<DropTriggerPlanNode>(new DropTriggerPlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, trigger_oid_, if_exists_));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

    /**
     * OID of namespace
     */
    catalog::namespace_oid_t namespace_oid_;

    /**
     * OID of the trigger to drop
     */
    catalog::trigger_oid_t trigger_oid_;

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
   * @param trigger_oid OID of the trigger to drop
   */
  DropTriggerPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                      std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                      catalog::namespace_oid_t namespace_oid, catalog::trigger_oid_t trigger_oid, bool if_exists)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        trigger_oid_(trigger_oid),
        if_exists_(if_exists) {}

 public:
  /**
   * Default constructor for deserialization
   */
  DropTriggerPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropTriggerPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_TRIGGER; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the trigger to drop
   */
  catalog::trigger_oid_t GetTriggerOid() const { return trigger_oid_; }

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
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * OID of the trigger to drop
   */
  catalog::trigger_oid_t trigger_oid_;

  /**
   * Whether "IF EXISTS" was used
   */
  bool if_exists_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DropTriggerPlanNode);

}  // namespace noisepage::planner
