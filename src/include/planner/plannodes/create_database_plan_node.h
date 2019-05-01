#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/select_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for creating databases
 */
class CreateDatabasePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create database plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param database_name the name of the database
     * @return builder object
     */
    Builder &SetDatabaseName(std::string database_name) {
      database_name_ = std::move(database_name);
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kDatabase) {
        database_name_ = std::string(create_stmt->GetDatabaseName());
      }
      return *this;
    }

    /**
     * Build the create database plan node
     * @return plan node
     */
    std::shared_ptr<CreateDatabasePlanNode> Build() {
      return std::shared_ptr<CreateDatabasePlanNode>(
          new CreateDatabasePlanNode(std::move(children_), std::move(output_schema_), std::move(database_name_)));
    }

   protected:
    /**
     * Database Name
     */
    std::string database_name_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_name the name of the database
   */
  CreateDatabasePlanNode(std::vector<std::shared_ptr<AbstractPlanNode>> &&children,
                         std::shared_ptr<OutputSchema> output_schema, std::string database_name)
      : AbstractPlanNode(std::move(children), std::move(output_schema)), database_name_(std::move(database_name)) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  CreateDatabasePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateDatabasePlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_DATABASE; }

  /**
   * @return name of the database for
   */
  const std::string &GetDatabaseName() const { return database_name_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  void FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Database Name
   */
  std::string database_name_;
};

DEFINE_JSON_DECLARATIONS(CreateDatabasePlanNode);

}  // namespace terrier::planner
