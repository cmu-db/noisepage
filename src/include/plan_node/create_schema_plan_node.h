#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for creating schemas
 */
class CreateSchemaPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create schema plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param schema_name the name of the schema
     * @return builder object
     */
    Builder &SetSchemaName(std::string schema_name) {
      schema_name_ = std::move(schema_name);
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kSchema) {
        schema_name_ = std::string(create_stmt->GetSchemaName());
      }
      return *this;
    }

    /**
     * Build the create schema plan node
     * @return plan node
     */
    std::unique_ptr<CreateSchemaPlanNode> Build() {
      return std::unique_ptr<CreateSchemaPlanNode>(
          new CreateSchemaPlanNode(std::move(children_), std::move(output_schema_), std::move(schema_name_)));
    }

   protected:
    /**
     * namespace Name
     */
    std::string schema_name_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param schema_name the name of the schema
   */
  CreateSchemaPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                       std::shared_ptr<OutputSchema> output_schema, std::string schema_name)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), schema_name_(std::move(schema_name)) {}

 public:
  CreateSchemaPlanNode() = delete;
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_SCHEMA; }

  /**
   * @return name of the schema
   */
  const std::string &GetSchemaName() const { return schema_name_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * Schema Name
   */
  std::string schema_name_;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CreateSchemaPlanNode);
};

}  // namespace terrier::plan_node
