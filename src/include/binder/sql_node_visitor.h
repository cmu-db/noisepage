#pragma once

#include "common/managed_pointer.h"

namespace noisepage {

namespace parser {
class ParseResult;

class SelectStatement;
class CreateStatement;
class CreateFunctionStatement;
class InsertStatement;
class DeleteStatement;
class DropStatement;
class ExplainStatement;
class PrepareStatement;
class ExecuteStatement;
class TransactionStatement;
class UpdateStatement;
class CopyStatement;
class AnalyzeStatement;
class VariableSetStatement;
class JoinDefinition;
class TableRef;

class GroupByDescription;
class OrderByDescription;
class LimitDescription;

class AggregateExpression;
class CaseExpression;
class ColumnValueExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantValueExpression;
class DefaultValueExpression;
class DerivedValueExpression;
class FunctionExpression;
class OperatorExpression;
class ParameterValueExpression;
class StarExpression;
class TableStarExpression;
class SubqueryExpression;
class TypeCastExpression;
}  // namespace parser

namespace binder {

/**
 * Visitor pattern definitions for the parser statements.
 */
class SqlNodeVisitor {
 public:
  /**
   * Virtual destructor for SqlNodeVisitor.
   */
  virtual ~SqlNodeVisitor() = default;

  /**
   * Visitor pattern for AnalyzeStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::AnalyzeStatement> node) {}

  /**
   * Visitor pattern for CopyStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::CopyStatement> node) {}

  /**
   * Visitor pattern for CreateFunctionStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::CreateFunctionStatement> node) {}

  /**
   * Visitor pattern for CreateStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::CreateStatement> node) {}

  /**
   * Visitor pattern for DeleteStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::DeleteStatement> node) {}

  /**
   * Visitor pattern for DropStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::DropStatement> node) {}

  /**
   * Visitor pattern for ExecuteStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ExecuteStatement> node) {}

  /**
   * Visitor pattern for ExplainStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ExplainStatement> node) {}

  /**
   * Visitor pattern for InsertStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::InsertStatement> node) {}

  /**
   * Visitor pattern for PrepareStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::PrepareStatement> node) {}

  /**
   * Visitor pattern for SelectStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::SelectStatement> node) {}

  /**
   * Visitor pattern for TransactionStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::TransactionStatement> node) {}

  /**
   * Visitor pattern for UpdateStatement.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::UpdateStatement> node) {}

  /**
   * Visitor pattern for VariableSetStatement
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::VariableSetStatement> node) {}

  /**
   * Visitor pattern for AggregateExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::AggregateExpression> expr);

  /**
   * Visitor pattern for CaseExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::CaseExpression> expr);

  /**
   * Visitor pattern for ColumnValueExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ColumnValueExpression> expr);

  /**
   * Visitor pattern for ComparisonExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ComparisonExpression> expr);

  /**
   * Visitor pattern for ConjunctionExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ConjunctionExpression> expr);

  /**
   * Visitor pattern for ConstantValueExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ConstantValueExpression> expr);

  /**
   * Visitor pattern for DefaultValueExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::DefaultValueExpression> expr);

  /**
   * Visitor pattern for DerivedValueExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::DerivedValueExpression> expr);

  /**
   * Visitor pattern for FunctionExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::FunctionExpression> expr);

  /**
   * Visitor pattern for OperatorExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::OperatorExpression> expr);

  /**
   * Visitor pattern for ParameterValueExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::ParameterValueExpression> expr);

  /**
   * Visitor pattern for StarExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::StarExpression> expr);

  /**
   * Visitor pattern for TableStarExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::TableStarExpression> expr);

  /**
   * Visitor pattern for SubqueryExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::SubqueryExpression> expr);

  /**
   * Visitor pattern for TypeCastExpression
   * @param expr to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::TypeCastExpression> expr);

  // START some sub query nodes inside SelectStatement

  /**
   * Visitor pattern for GroupByDescription.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::GroupByDescription> node) {}

  /**
   * Visitor pattern for JoinDefinition.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::JoinDefinition> node) {}

  /**
   * Visitor pattern for LimitDescription.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::LimitDescription> node) {}

  /**
   * Visitor pattern for OrderByDescription.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::OrderByDescription> node) {}

  /**
   * Visitor pattern for TableRef.
   * @param node node to be visited
   */
  virtual void Visit(common::ManagedPointer<parser::TableRef> node) {}

  // END some sub query nodes inside SelectStatement
};

}  // namespace binder
}  // namespace noisepage
