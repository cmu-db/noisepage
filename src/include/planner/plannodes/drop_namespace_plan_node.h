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
 *  The plan node for dropping namespaces
 */
class DropNamespacePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a drop namespace plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param namespace_oid the OID of the namespace to drop
     * @return builder object
     */
    Builder &SetNamespaceOid(catalog::namespace_oid_t namespace_oid) {
      namespace_oid_ = namespace_oid;
      return *this;
    }

    /**
     * Build the drop namespace plan node
     * @return plan node
     */
    std::unique_ptr<DropNamespacePlanNode> Build();

   protected:
    /**
     * OID of the namespace to drop
     */
    catalog::namespace_oid_t namespace_oid_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace to drop
   */
  DropNamespacePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                        std::unique_ptr<OutputSchema> output_schema, catalog::namespace_oid_t namespace_oid);

 public:
  /**
   * Default constructor for deserialization
   */
  DropNamespacePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(DropNamespacePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::DROP_NAMESPACE; }

  /**
   * @return OID of the namespace to drop
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

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
   * OID of the namespace to drop
   */
  catalog::namespace_oid_t namespace_oid_;
};

DEFINE_JSON_HEADER_DECLARATIONS(DropNamespacePlanNode);

}  // namespace noisepage::planner
