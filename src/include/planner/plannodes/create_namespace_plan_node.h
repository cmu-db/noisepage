#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for creating namespaces
 */
class CreateNamespacePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create namespace plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param namespace_name name of the namespace
     * @return builder object
     */
    Builder &SetNamespaceName(std::string namespace_name) {
      namespace_name_ = std::move(namespace_name);
      return *this;
    }

    /**
     * Build the create namespace plan node
     * @return plan node
     */
    std::unique_ptr<CreateNamespacePlanNode> Build() {
      return std::unique_ptr<CreateNamespacePlanNode>(
          new CreateNamespacePlanNode(std::move(children_), std::move(output_schema_), std::move(namespace_name_)));
    }

   protected:
    /**
     * Namespace name
     */
    std::string namespace_name_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_name name of the namespace
   */
  CreateNamespacePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                          std::unique_ptr<OutputSchema> output_schema, std::string namespace_name)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), namespace_name_(std::move(namespace_name)) {}

 public:
  /**
   * Default constructor for deserialization
   */
  CreateNamespacePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateNamespacePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_NAMESPACE; }

  /**
   * @return name of the namespace
   */
  const std::string &GetNamespaceName() const { return namespace_name_; }

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
   * Namespace name
   */
  std::string namespace_name_;
};

DEFINE_JSON_HEADER_DECLARATIONS(CreateNamespacePlanNode);

}  // namespace noisepage::planner
