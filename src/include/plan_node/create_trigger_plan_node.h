#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "plan_node/abstract_plan_node.h"

namespace terrier::plan_node {

/**
 * Plan node for creating triggers
 */
class CreateTriggerPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for a create trigger plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * @param trigger_name name of the trigger
     * @return builder object
     */
    Builder &SetTriggerName(std::string trigger_name) {
      trigger_name_ = std::move(trigger_name);
      return *this;
    }

    /**
     * @param trigger_funcnames trigger function names
     * @return builder object
     */
    Builder &SetTriggerFuncnames(std::vector<std::string> &&trigger_funcnames) {
      trigger_funcnames_ = std::move(trigger_funcnames);
      return *this;
    }

    /**
     * @param trigger_args trigger args
     * @return builder object
     */
    Builder &SetTriggerArgs(std::vector<std::string> &&trigger_args) {
      trigger_args_ = std::move(trigger_args);
      return *this;
    }

    /**
     * @param trigger_columns trigger columns
     * @return builder object
     */
    Builder &SetTriggerColumns(std::vector<std::string> &&trigger_columns) {
      trigger_columns_ = std::move(trigger_columns);
      return *this;
    }

    /**
     * @param trigger_when trigger when clause
     * @return builder object
     */
    Builder &SetTriggerWhen(std::shared_ptr<parser::AbstractExpression> trigger_when) {
      trigger_when_ = std::move(trigger_when);
      return *this;
    }

    /**
     * @param trigger_type trigger type, i.e. information about row, timing, events, access by pg_trigger
     * @return builder object
     */
    Builder &SetTriggerType(int16_t trigger_type) {
      trigger_type_ = trigger_type;
      return *this;
    }

    /**
     * @param create_stmt the SQL CREATE statement
     * @return builder object
     */
    Builder &SetFromCreateStatement(parser::CreateStatement *create_stmt) {
      if (create_stmt->GetCreateType() == parser::CreateStatement::CreateType::kTrigger) {
        trigger_name_ = std::string(create_stmt->GetTriggerName());
        table_name_ = std::string(create_stmt->GetTableName());
        schema_name_ = std::string(create_stmt->GetSchemaName());

        if (create_stmt->GetTriggerWhen()) {
          trigger_when_ = create_stmt->GetTriggerWhen()->Copy();
        } else {
          trigger_when_ = nullptr;
        }
        trigger_type_ = create_stmt->GetTriggerType();

        for (auto &s : create_stmt->GetTriggerFuncNames()) {
          trigger_funcnames_.push_back(s);
        }
        for (auto &s : create_stmt->GetTriggerArgs()) {
          trigger_args_.push_back(s);
        }
        for (auto &s : create_stmt->GetTriggerColumns()) {
          trigger_columns_.push_back(s);
        }
      }
      return *this;
    }

    /**
     * Build the create function plan node
     * @return plan node
     */
    std::unique_ptr<CreateTriggerPlanNode> Build() {
      return std::unique_ptr<CreateTriggerPlanNode>(new CreateTriggerPlanNode(
          std::move(children_), std::move(output_schema_), std::move(table_name_), std::move(schema_name_),
          std::move(trigger_name_), std::move(trigger_funcnames_), std::move(trigger_args_),
          std::move(trigger_columns_), std::move(trigger_when_), trigger_type_));
    }

   protected:
    /**
     * Table Name
     */
    std::string table_name_;

    /**
     * namespace Name
     */
    std::string schema_name_;

    /**
     * Name of the trigger
     */
    std::string trigger_name_;

    /**
     * Names of the trigger functions
     */
    std::vector<std::string> trigger_funcnames_;

    /**
     * Trigger arguments
     */
    std::vector<std::string> trigger_args_;

    /**
     * Trigger columns
     */
    std::vector<std::string> trigger_columns_;

    /**
     * Trigger when claus
     */
    std::shared_ptr<parser::AbstractExpression> trigger_when_;

    /**
     * Type of trigger
     */
    int16_t trigger_type_ = 0;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param table_name the name of the table
   * @param schema_name the name of the schema
   * @param trigger_name name of the trigger
   * @param trigger_funcnames trigger function names
   * @param trigger_args trigger args
   * @param trigger_columns trigger columns
   * @param trigger_when trigger when clause
   * @param trigger_type trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  CreateTriggerPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                        std::shared_ptr<OutputSchema> output_schema, std::string table_name, std::string schema_name,
                        std::string trigger_name, std::vector<std::string> &&trigger_funcnames,
                        std::vector<std::string> &&trigger_args, std::vector<std::string> &&trigger_columns,
                        std::shared_ptr<parser::AbstractExpression> &&trigger_when, int16_t trigger_type)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        table_name_(std::move(table_name)),
        schema_name_(std::move(schema_name)),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(std::move(trigger_when)),
        trigger_type_(trigger_type) {}

 public:
  CreateTriggerPlanNode() = delete;
  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_TRIGGER; }

  /**
   * @return name of the table
   */
  const std::string &GetTableName() const { return table_name_; }
  /**
   * @return name of the schema
   */
  const std::string &GetSchemaName() const { return schema_name_; }

  /**
   * @return trigger name
   */
  std::string GetTriggerName() const { return trigger_name_; }

  /**
   * @return trigger function names
   */
  std::vector<std::string> GetTriggerFuncName() const { return trigger_funcnames_; }

  /**
   * @return trigger args
   */
  std::vector<std::string> GetTriggerArgs() const { return trigger_args_; }

  /**
   * @return trigger columns
   */
  std::vector<std::string> GetTriggerColumns() const { return trigger_columns_; }

  /**
   * @return trigger when clause
   */
  std::shared_ptr<parser::AbstractExpression> GetTriggerWhen() const { return trigger_when_; }

  /**
   * @return trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  int16_t GetTriggerType() const { return trigger_type_; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override;

  bool operator==(const AbstractPlanNode &rhs) const override;

 private:
  /**
   * Table Name
   */
  std::string table_name_;

  /**
   * namespace Name
   */
  std::string schema_name_;

  /**
   * Name of the trigger
   */
  std::string trigger_name_;

  /**
   * Names of the trigger functions
   */
  std::vector<std::string> trigger_funcnames_;

  /**
   * Trigger arguments
   */
  std::vector<std::string> trigger_args_;

  /**
   * Trigger columns
   */
  std::vector<std::string> trigger_columns_;

  /**
   * Trigger when claus
   */
  std::shared_ptr<parser::AbstractExpression> trigger_when_;

  /**
   * Type of trigger
   */
  int16_t trigger_type_ = 0;

 public:
  /**
   * Don't allow plan to be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CreateTriggerPlanNode);
};

}  // namespace terrier::plan_node
