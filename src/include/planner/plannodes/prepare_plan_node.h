#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::planner {

/**
 * Plan node for prepare
 */
class PreparePlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a prepare plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param name the name of the Prepared Statement
     * @return builder object
     */
    Builder &SetName(std::string name) {
      name_ = std::move(name);
      return *this;
    }

    /**
     * @param dml_statement the underlying DML statement
     * @return builder object
     */
    Builder &SetDMLStatement(common::ManagedPointer<parser::SQLStatement> dml_statement) {
      dml_statement_ = dml_statement;
      return *this;
    }

    /**
     * @param parameters the parameters in order of appearance in the DML statement
     * @return builder object
     */
    Builder &SetParameters(
        common::ManagedPointer<std::vector<common::ManagedPointer<parser::ParameterValueExpression>>> parameters) {
      parameters_ = parameters;
      return *this;
    }

    /**
     * @param block_store the underlying storage space for the temporary table for storing Prepared Statements
     * @return builder object
     */
    Builder &SetBlockStore(common::ManagedPointer<storage::BlockStore> block_store) {
      block_store_ = block_store;
      return *this;
    }

    /**
     * Build the prepare plan node
     * @return plan node
     */
    std::unique_ptr<PreparePlanNode> Build() {
      return std::unique_ptr<PreparePlanNode>(new PreparePlanNode(std::move(children_), std::move(output_schema_),
                                                                  std::move(name_), dml_statement_, parameters_,
                                                                  block_store_));
    }

   protected:
    /**
     * The name of this Prepared Statement
     */
    std::string name_;

    /**
     * The underlying DML statement
     */
    common::ManagedPointer<parser::SQLStatement> dml_statement_;

    /**
     * The parameters in order of appearance in the DML statement
     */
    common::ManagedPointer<std::vector<common::ManagedPointer<parser::ParameterValueExpression>>> parameters_;

    /**
     * The underlying storage space for the temporary table for storing Prepared Statements
     */
    common::ManagedPointer<storage::BlockStore> block_store_;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_name the name of the database
   * @param storage space for the temporary table
   */
  PreparePlanNode(
      std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema,
      std::string name, common::ManagedPointer<parser::SQLStatement> dml_statement,
      common::ManagedPointer<std::vector<common::ManagedPointer<parser::ParameterValueExpression>>> parameters,
      common::ManagedPointer<storage::BlockStore> block_store)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        name_(std::move(name)),
        dml_statement_(dml_statement),
        parameters_(parameters),
        block_store_(block_store) {}

 public:
  /**
   * Default constructor used for deserialization
   */
  PreparePlanNode() = default;

  DISALLOW_COPY_AND_MOVE(PreparePlanNode)

  /**
   * @return name of the Prepared Statement
   */
  const std::string &GetPreparedName() const { return name_; }

  /**
   * @return the underlying DML statement
   */
  const common::ManagedPointer<parser::SQLStatement> &GetDMLStatement() { return dml_statement_; }

  /**
   * @return the vector of parameters in the DML statement
   */
  const common::ManagedPointer<std::vector<common::ManagedPointer<parser::ParameterValueExpression>>> &GetParameters()
      const {
    return parameters_;
  }

  /**
   * @return block store to be used when constructing a temporary table
   */
  common::ManagedPointer<storage::BlockStore> GetBlockStore() const { return block_store_; }

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::PREPARE; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

  nlohmann::json ToJson() const override;
  std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /**
   * The name of this Prepared Statement
   */
  std::string name_;

  /**
   * The underlying DML statement
   */
  common::ManagedPointer<parser::SQLStatement> dml_statement_;

  /**
   * The parameters in order of appearance in the DML statement
   */
  common::ManagedPointer<std::vector<common::ManagedPointer<parser::ParameterValueExpression>>> parameters_;

  /**
   * The underlying storage space for the temporary table for storing Prepared Statements
   */
  common::ManagedPointer<storage::BlockStore> block_store_;
};

DEFINE_JSON_DECLARATIONS(PreparePlanNode);

}  // namespace terrier::planner
