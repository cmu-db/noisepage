#pragma once

#include <memory>
#include <string>
#include "binder/binder_context.h"
#include "catalog/catalog_defs.h"
#include "common/sql_node_visitor.h"
#include "parser/statements.h"

namespace terrier {

namespace parser {
class SQLStatement;
class CaseExpression;
class ConstantValueExpression;
class ColumnValueExpression;
class SubqueryExpression;
class StarExpression;
class OperatorExpression;
class AggregateExpression;
}  // namespace parser

namespace catalog {
class CatalogAccessor;
}  // namespace catalog

namespace binder {

/**
 * Interface to be notified of the composition of a bind node.
 */
class BindNodeVisitor : public SqlNodeVisitor {
 public:
  /**
   * Initialize the bind node visitor object with a pointer to a catalog accessor, and a default database name
   * @param catalog_accessor Pointer to a catalog accessor
   * @param default_database_name Default database name
   */
  BindNodeVisitor(std::unique_ptr<catalog::CatalogAccessor> catalog_accessor, std::string default_database_name);
  ~BindNodeVisitor() override { delete context_; }

  /**
   * Perform binding on the passed in tree. Bind the ids according to the names in the tree.
   * For example, bind the corresponding database oid to an expression, which has a database name
   * @param tree Parsed in AST tree of the SQL statement
   */
  void BindNameToNode(parser::SQLStatement *tree);
  void Visit(parser::SelectStatement *node) override;

  // Some sub query nodes inside SelectStatement
  void Visit(parser::JoinDefinition *node) override;
  void Visit(parser::TableRef *node) override;
  void Visit(parser::GroupByDescription *node) override;
  void Visit(parser::OrderByDescription *node) override;
  void Visit(parser::LimitDescription *node) override;

  void Visit(parser::CreateStatement *node) override;
  void Visit(parser::CreateFunctionStatement *node) override;
  void Visit(parser::InsertStatement *node) override;
  void Visit(parser::DeleteStatement *node) override;
  void Visit(parser::DropStatement *node) override;
  void Visit(parser::PrepareStatement *node) override;
  void Visit(parser::ExecuteStatement *node) override;
  void Visit(parser::TransactionStatement *node) override;
  void Visit(parser::UpdateStatement *node) override;
  void Visit(parser::CopyStatement *node) override;
  void Visit(parser::AnalyzeStatement *node) override;

  void Visit(parser::CaseExpression *expr) override;
  void Visit(parser::SubqueryExpression *expr) override;

  void Visit(parser::ConstantValueExpression *expr) override;
  void Visit(parser::ColumnValueExpression *expr) override;
  void Visit(parser::StarExpression *expr) override;
  //  void Visit(parser::FunctionExpression *expr) override;

  // Deduce value type for these expressions
  void Visit(parser::OperatorExpression *expr) override;
  void Visit(parser::AggregateExpression *expr) override;

 private:
  BinderContext *context_ = nullptr;
  std::unique_ptr<catalog::CatalogAccessor> catalog_accessor_;
  std::string default_database_name_;
};

}  // namespace binder
}  // namespace terrier
