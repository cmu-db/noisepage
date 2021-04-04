#pragma once

#include <memory>
#include <string>
#include <vector>

#include "binder/sql_node_visitor.h"
#include "catalog/catalog_defs.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "parser/postgresparser.h"
#include "parser/select_statement.h"
#include "type/type_id.h"

namespace noisepage {

namespace parser {
class AbstractExpression;
class AggregateExpression;
class CaseExpression;
class ConstantValueExpression;
class ColumnValueExpression;
class OperatorExpression;
class SubqueryExpression;
class StarExpression;
class TableStarExpression;
class SQLStatement;
}  // namespace parser

namespace catalog {
class CatalogAccessor;
class Schema;
}  // namespace catalog

namespace binder {
class BinderContext;
class BinderSherpa;

/**
 * Interface to be notified of the composition of a bind node.
 */
class BindNodeVisitor final : public SqlNodeVisitor {
 public:
  /**
   * Initialize the bind node visitor object with a pointer to a catalog accessor, and a default database name
   * @param catalog_accessor Pointer to a catalog accessor
   * @param db_oid oid of the connected database
   */
  BindNodeVisitor(common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor, catalog::db_oid_t db_oid);

  /** Destructor. Must be defined due to forward declaration. */
  ~BindNodeVisitor() final;

  /**
   * TODO(Kyle): Document.
   */
  std::unordered_map<std::string, std::pair<std::string, std::size_t>> BindAndGetUDFParams(
      common::ManagedPointer<parser::ParseResult> parse_result,
      common::ManagedPointer<execution::ast::udf::UDFASTContext> udf_ast_context);

  /**
   * Perform binding on the passed in tree. Bind the relation names to oids
   * @param parse_result Result generated by the parser. A collection of statements and expressions in the query
   * @param parameters parameters for the query being bound, can be nullptr if there are no parameters
   * @param desired_parameter_types same size as parameters, can be nullptr if there are no parameters
   */
  void BindNameToNode(common::ManagedPointer<parser::ParseResult> parse_result,
                      common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters,
                      common::ManagedPointer<std::vector<type::TypeId>> desired_parameter_types);

  void Visit(common::ManagedPointer<parser::AnalyzeStatement> node) override;
  void Visit(common::ManagedPointer<parser::CopyStatement> node) override;
  void Visit(common::ManagedPointer<parser::CreateFunctionStatement> node) override;
  void Visit(common::ManagedPointer<parser::CreateStatement> node) override;
  void Visit(common::ManagedPointer<parser::DeleteStatement> node) override;
  void Visit(common::ManagedPointer<parser::DropStatement> node) override;
  void Visit(common::ManagedPointer<parser::ExecuteStatement> node) override;
  void Visit(common::ManagedPointer<parser::ExplainStatement> node) override;
  void Visit(common::ManagedPointer<parser::InsertStatement> node) override;
  void Visit(common::ManagedPointer<parser::PrepareStatement> node) override;
  void Visit(common::ManagedPointer<parser::SelectStatement> node) override;
  void Visit(common::ManagedPointer<parser::TransactionStatement> node) override;
  void Visit(common::ManagedPointer<parser::UpdateStatement> node) override;
  void Visit(common::ManagedPointer<parser::VariableSetStatement> node) override;

  void Visit(common::ManagedPointer<parser::AggregateExpression> expr) override;
  void Visit(common::ManagedPointer<parser::CaseExpression> expr) override;
  void Visit(common::ManagedPointer<parser::ColumnValueExpression> expr) override;
  void Visit(common::ManagedPointer<parser::ComparisonExpression> expr) override;
  void Visit(common::ManagedPointer<parser::ConjunctionExpression> expr) override;
  void Visit(common::ManagedPointer<parser::ConstantValueExpression> expr) override;
  void Visit(common::ManagedPointer<parser::DefaultValueExpression> expr) override;
  void Visit(common::ManagedPointer<parser::DerivedValueExpression> expr) override;
  void Visit(common::ManagedPointer<parser::FunctionExpression> expr) override;
  void Visit(common::ManagedPointer<parser::OperatorExpression> expr) override;
  void Visit(common::ManagedPointer<parser::ParameterValueExpression> expr) override;
  void Visit(common::ManagedPointer<parser::StarExpression> expr) override;
  void Visit(common::ManagedPointer<parser::TableStarExpression> expr) override;
  void Visit(common::ManagedPointer<parser::SubqueryExpression> expr) override;
  void Visit(common::ManagedPointer<parser::TypeCastExpression> expr) override;

  void Visit(common::ManagedPointer<parser::GroupByDescription> node) override;
  void Visit(common::ManagedPointer<parser::JoinDefinition> node) override;
  void Visit(common::ManagedPointer<parser::LimitDescription> node) override;
  void Visit(common::ManagedPointer<parser::OrderByDescription> node) override;
  void Visit(common::ManagedPointer<parser::TableRef> node) override;

 private:
  /** BinderSherpa which stores metadata (e.g. type information) across Visit calls. **/
  std::unique_ptr<BinderSherpa> sherpa_;
  /** Current context of the query or subquery */
  common::ManagedPointer<BinderContext> context_ = nullptr;

  /** Context for UDF AST */
  common::ManagedPointer<execution::ast::udf::UDFASTContext> udf_ast_context_{};
  /** Parameters for UDF */
  std::unordered_map<std::string, std::pair<std::string, size_t>> udf_params_;

  /** Catalog accessor */
  const common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor_;
  const catalog::db_oid_t db_oid_;

  static void InitTableRef(common::ManagedPointer<parser::TableRef> node);

  /**
   * Change the type of exprs_ of order_by_description from ConstantValueExpression to ColumnValueExpression.
   * @param order_by_description OrderByDescription
   * @param select_items select columns
   */
  void UnifyOrderByExpression(common::ManagedPointer<parser::OrderByDescription> order_by_description,
                              const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_items);

  void ValidateDatabaseName(const std::string &db_name);

  /**
   * Validate values that are being inserted into table and add any default/null values for missing columns
   * @param node InsertStatement to validate
   * @param values values that are being inserted
   * @param table_schema schema of table being inserted into
   */
  void ValidateAndCorrectInsertValues(common::ManagedPointer<parser::InsertStatement> node,
                                      std::vector<common::ManagedPointer<parser::AbstractExpression>> *values,
                                      const catalog::Schema &table_schema);
};

}  // namespace binder
}  // namespace noisepage
