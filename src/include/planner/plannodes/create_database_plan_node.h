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
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
     * Build the create database plan node
     * @return plan node
     */
    std::unique_ptr<CreateDatabasePlanNode> Build();

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
   * @param plan_node_id Plan node id
   */
  CreateDatabasePlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, std::string database_name,
                         plan_node_id_t plan_node_id);

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

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * Database Name
   */
  std::string database_name_;
};

DEFINE_JSON_HEADER_DECLARATIONS(CreateDatabasePlanNode);

}  // namespace noisepage::planner
