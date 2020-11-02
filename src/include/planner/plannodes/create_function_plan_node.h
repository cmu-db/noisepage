#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "parser/create_function_statement.h"
#include "parser/parser_defs.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for creating user defined functions
 */
class CreateFunctionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for an create function plan node
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
     * @param language the UDF language type
     * @return builder object
     */
    Builder &SetLanguage(parser::PLType language) {
      language_ = language;
      return *this;
    }

    /**
     * @param function_param_names Function parameters names passed to the UDF
     * @return builder object
     */
    Builder &SetFunctionParamNames(std::vector<std::string> &&function_param_names) {
      function_param_names_ = std::move(function_param_names);
      return *this;
    }

    /**
     * @param function_param_types Function parameter types passed to the UDF
     * @return builder object
     */
    Builder &SetFunctionParamTypes(std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types) {
      function_param_types_ = std::move(function_param_types);
      return *this;
    }

    /**
     * @param function_body query string/function body of the UDF
     * @return builder object
     */
    Builder &SetBody(std::vector<std::string> &&function_body) {
      function_body_ = std::move(function_body);
      return *this;
    }

    /**
     * @param is_replace indicates if the function definition needs to be replaced
     * @return builder object
     */
    Builder &SetIsReplace(bool is_replace) {
      is_replace_ = is_replace;
      return *this;
    }

    /**
     * @param function_name function name of the UDF
     * @return builder object
     */
    Builder &SetFunctionName(std::string function_name) {
      function_name_ = std::move(function_name);
      return *this;
    }

    /**
     * @param return_type return type of the UDF
     * @return builder object
     */
    Builder &SetReturnType(parser::BaseFunctionParameter::DataType return_type) {
      return_type_ = return_type;
      return *this;
    }

    /**
     * @param param_count number of parameter of UDF
     * @return builder object
     */
    Builder &SetParamCount(int param_count) {
      param_count_ = param_count;
      return *this;
    }

    /**
     * Build the create function plan node
     * @return plan node
     */
    std::unique_ptr<CreateFunctionPlanNode> Build();

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
     * Indicates the UDF language type
     */
    parser::PLType language_;

    /**
     * Function parameters names passed to the UDF
     */
    std::vector<std::string> function_param_names_;

    /**
     * Function parameter types passed to the UDF
     */
    std::vector<parser::BaseFunctionParameter::DataType> function_param_types_;

    /**
     * Query string/ function body of the UDF
     */
    std::vector<std::string> function_body_;

    /**
     * Indicates if the function definition needs to be replaced
     */
    bool is_replace_;

    /**
     * Function name of the UDF
     */
    std::string function_name_;

    /**
     * Return type of the UDF
     */
    parser::BaseFunctionParameter::DataType return_type_;

    /**
     * Number of parameters
     */
    int param_count_ = 0;
  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   * @param database_oid OID of the database
   * @param namespace_oid OID of the namespace
   * @param language the UDF language type
   * @param function_param_names Function parameters names passed to the UDF
   * @param function_param_types Function parameter types passed to the UDF
   * @param function_body query string/function body of the UDF
   * @param is_replace indicates if the function definition needs to be replaced
   * @param function_name function name of the UDF
   * @param return_type return type of the UDF
   * @param param_count number of parameter of UDF
   */
  CreateFunctionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children,
                         std::unique_ptr<OutputSchema> output_schema, catalog::db_oid_t database_oid,
                         catalog::namespace_oid_t namespace_oid, parser::PLType language,
                         std::vector<std::string> &&function_param_names,
                         std::vector<parser::BaseFunctionParameter::DataType> &&function_param_types,
                         std::vector<std::string> &&function_body, bool is_replace, std::string function_name,
                         parser::BaseFunctionParameter::DataType return_type, int param_count);

 public:
  /**
   * Default constructor used for deserialization
   */
  CreateFunctionPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(CreateFunctionPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::CREATE_FUNC; }

  /**
   * @return OID of the database
   */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /**
   * @return OID of the namespace
   */
  catalog::namespace_oid_t GetNamespaceOid() const { return namespace_oid_; }

  /**
   * @return name of the user defined function
   */
  std::string GetFunctionName() const { return function_name_; }

  /**
   * @return language type of the user defined function
   */
  parser::PLType GetUDFLanguage() const { return language_; }

  /**
   * @return body of the user defined function
   */
  std::vector<std::string> GetFunctionBody() const { return function_body_; }

  /**
   * @return parameter names of the user defined function
   */
  std::vector<std::string> GetFunctionParameterNames() const { return function_param_names_; }

  /**
   * @return parameter types of the user defined function
   */
  std::vector<parser::BaseFunctionParameter::DataType> GetFunctionParameterTypes() const {
    return function_param_types_;
  }

  /**
   * @return return type of the user defined function
   */
  parser::BaseFunctionParameter::DataType GetReturnType() const { return return_type_; }

  /**
   * @return whether the definition of the user defined function needs to be replaced
   */
  bool IsReplace() const { return is_replace_; }

  /**
   * @return number of parameters of the user defined function
   */
  int GetParamCount() const { return param_count_; }

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
   * OID of database
   */
  catalog::db_oid_t database_oid_;

  /**
   * OID of namespace
   */
  catalog::namespace_oid_t namespace_oid_;

  /**
   * Indicates the UDF language type
   */
  parser::PLType language_;

  /**
   * Function parameters names passed to the UDF
   */
  std::vector<std::string> function_param_names_;

  /**
   * Function parameter types passed to the UDF
   */
  std::vector<parser::BaseFunctionParameter::DataType> function_param_types_;

  /**
   * Query string/ function body of the UDF
   */
  std::vector<std::string> function_body_;

  /**
   * Indicates if the function definition needs to be replaced
   */
  bool is_replace_;

  /**
   * Function name of the UDF
   */
  std::string function_name_;

  /**
   * Return type of the UDF
   */
  parser::BaseFunctionParameter::DataType return_type_;

  /**
   * Number of parameters
   */
  int param_count_ = 0;
};

DEFINE_JSON_HEADER_DECLARATIONS(CreateFunctionPlanNode);

}  // namespace noisepage::planner
