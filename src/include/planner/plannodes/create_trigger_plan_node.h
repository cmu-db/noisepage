#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/create_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

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
     * @param database_oid  OID of the database
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
     * @param table_oid OID of the table to create trigger on
     * @return builder object
     */
    Builder &SetTableOid(catalog::table_oid_t table_oid) {
      table_oid_ = table_oid;
      return *this;
    }

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
    Builder &SetTriggerColumns(std::vector<catalog::col_oid_t> &&trigger_columns) {
      trigger_columns_ = std::move(trigger_columns);
      return *this;
    }

    /**
     * @param trigger_when trigger when clause
     * @return builder object
     */
    Builder &SetTriggerWhen(common::ManagedPointer<parser::AbstractExpression> trigger_when) {
      trigger_when_ = trigger_when;
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
     * Build the create trigger plan node
     * @return plan node
     */
    std::unique_ptr<CreateTriggerPlanNode> Build() {
      return std::unique_ptr<CreateTriggerPlanNode>(new CreateTriggerPlanNode(
          std::move(children_), std::move(output_schema_), database_oid_, namespace_oid_, table_oid_,
          std::move(trigger_name_), std::move(trigger_funcnames_), std::move(trigger_args_),
          std::move(trigger_columns_), trigger_when_, trigger_type_));
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
     * OID of the table to create trigger on
     */
    catalog::table_oid_t table_oid_;

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
    std::vector<catalog::col_oid_t> trigger_columns_;

    /**
     * Trigger when clause
     */
    common::ManagedPointer<parser::AbstractExpression> trigger_when_;

    /**
     * Type of trigger
     */
    int16_t trigger_type_ = 0;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param table_oid OID of the table to create trigger on
   * @param trigger_name name of the trigger
   * @param trigger_funcnames trigger function names
   * @param trigger_args trigger args
   * @param trigger_columns trigger columns
   * @param trigger_when trigger when clause
   * @param trigger_type trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  CreateTriggerPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                        std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                        catalog::namespace_oid_t namespace_oid, catalog::table_oid_t table_oid,
                        std::string trigger_name, std::vector<std::string> &&trigger_funcnames,
                        std::vector<std::string> &&trigger_args, std::vector<catalog::col_oid_t> &&trigger_columns,
                        common::ManagedPointer<parser::AbstractExpression> trigger_when, int16_t trigger_type)
      : AbstractPlanNode(std::move(children), std::move(output_schema)),
        database_oid_(database_oid),
        namespace_oid_(namespace_oid),
        table_oid_(table_oid),
        trigger_name_(std::move(trigger_name)),
        trigger_funcnames_(std::move(trigger_funcnames)),
        trigger_args_(std::move(trigger_args)),
        trigger_columns_(std::move(trigger_columns)),
        trigger_when_(trigger_when),
        trigger_type_(trigger_type) {}

 public:
  /**
   * Default constructor for deserialization
   */
  CreateTriggerPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateTriggerPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_TRIGGER; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return OID of the table to create trigger on
   */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

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
  std::vector<catalog::col_oid_t> GetTriggerColumns() const { return trigger_columns_; }

  /**
   * @return trigger when clause
   */
  common::ManagedPointer<parser::AbstractExpression> GetTriggerWhen() const {
    return common::ManagedPointer(trigger_when_);
  }

  /**
   * @return trigger type, i.e. information about row, timing, events, access by pg_trigger
   */
  int16_t GetTriggerType() const { return trigger_type_; }

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
   * OID of the table to create trigger on
   */
  catalog::table_oid_t table_oid_;

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
  std::vector<catalog::col_oid_t> trigger_columns_;

  /**
   * Trigger when clause
   */
  common::ManagedPointer<parser::AbstractExpression> trigger_when_;

  /**
   * Type of trigger
   */
  int16_t trigger_type_ = 0;
};

DEFINE_JSON_HEADER_DECLARATIONS(CreateTriggerPlanNode);

}  // namespace noisepage::planner
