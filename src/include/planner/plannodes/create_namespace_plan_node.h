#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

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
     * @param database_oid  of the database
     * @return builder object
     */
    Builder &SetDatabaseOid(catalog::db_oid_t database_oid) {
      database_oid_ = database_oid;
      return *this;
    }

    /**
     * @param namespace_name name of the namespace
     * @return builder object
     */
    Builder &SetNamespaceName(std::string namespace_name) {
      namespace_name_ = std::move(namespace_name);
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kSchema) {
        namespace_name_ = std::string(create_stmt->GetSchemaName());
      }
      return *this;
    }

    /**
     * Build the create namespace plan node
     * @return plan node
     */
    std::shared_ptr<CreateNamespacePlanNode> Build() {
      return std::shared_ptr<CreateNamespacePlanNode>(new CreateNamespacePlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, std::move(namespace_name_)));
    }

   protected:
    /**
     * OID of the database
     */
    catalog::db_oid_t database_oid_;

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
  CreateNamespacePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                          std::shared_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                          std::string namespace_name)
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
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return name of the namespace
   */
  const std::string &GetNamespaceName() const { return namespace_name_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * OID of the database
   */
  catalog::db_oid_t database_oid_;

  /**
   * Namespace name
   */
  std::string namespace_name_;
};
DEFINE_JSON_DECLARATIONS(CreateNamespacePlanNode);
}  // namespace terrier::planner
