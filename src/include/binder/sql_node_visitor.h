#pragma once

#include "common/managed_pointer.h"

namespace terrier {

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
class ConcatExpression;
class ConjunctionExpression;
class ConstantValueExpression;
class DefaultValueExpression;
class DerivedValueExpression;
class FunctionExpression;
class OperatorExpression;
class ParameterValueExpression;
class StarExpression;
class SubqueryExpression;
class TypeCastExpression;
}  // namespace parser

namespace binder {

class BinderSherpa;

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
   * Visitor pattern for SelectStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::SelectStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  // Some sub query nodes inside SelectStatement
  /**
   * Visitor pattern for JoinDefinition.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::JoinDefinition> node, common::ManagedPointer<BinderSherpa> sherpa) {
  }

  /**
   * Visitor pattern for TableRef.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::TableRef> node, common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for GroupByDescription.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::GroupByDescription> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for OrderByDescription.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::OrderByDescription> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for LimitDescription.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::LimitDescription> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for CreateStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::CreateStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for CreateFunctionStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::CreateFunctionStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for InsertStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::InsertStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for DeleteStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::DeleteStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for DropStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::DropStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for PrepareStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::PrepareStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for ExecuteStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ExecuteStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for TransactionStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::TransactionStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for UpdateStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::UpdateStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for CopyStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::CopyStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for AnalyzeStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::AnalyzeStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for ExplainStatement.
   * @param node node to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ExplainStatement> node,
                     common::ManagedPointer<BinderSherpa> sherpa) {}

  /**
   * Visitor pattern for AggregateExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::AggregateExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for CaseExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::CaseExpression> expr, common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ColumnValueExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ColumnValueExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ComparisonExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ComparisonExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ConcatExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ConcatExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ConjunctionExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ConjunctionExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ConstantValueExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ConstantValueExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for DefaultValueExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::DefaultValueExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for DerivedValueExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::DerivedValueExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for FunctionExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::FunctionExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for OperatorExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::OperatorExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for ParameterValueExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::ParameterValueExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for StarExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::StarExpression> expr, common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for SubqueryExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::SubqueryExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);

  /**
   * Visitor pattern for TypeCastExpression
   * @param expr to be visited
   * @param sherpa The BinderSherpa for storing state through visitor pattern
   */
  virtual void Visit(common::ManagedPointer<parser::TypeCastExpression> expr,
                     common::ManagedPointer<BinderSherpa> sherpa);
};
}  // namespace binder
}  // namespace terrier
