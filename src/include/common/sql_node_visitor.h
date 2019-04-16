#pragma once

namespace terrier {

namespace parser {
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
}  // namespace parser

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
   */
  virtual void Visit(parser::SelectStatement *node) {}

  // Some sub query nodes inside SelectStatement
  /**
   * Visitor pattern for JoinDefinition.
   * @param node node to be visited
   */
  virtual void Visit(parser::JoinDefinition *node) {}

  /**
   * Visitor pattern for TableRef.
   * @param node node to be visited
   */
  virtual void Visit(parser::TableRef *node) {}

  /**
   * Visitor pattern for GroupByDescription.
   * @param node node to be visited
   */
  virtual void Visit(parser::GroupByDescription *node) {}

  /**
   * Visitor pattern for OrderByDescription.
   * @param node node to be visited
   */
  virtual void Visit(parser::OrderByDescription *node) {}

  /**
   * Visitor pattern for LimitDescription.
   * @param node node to be visited
   */
  virtual void Visit(parser::LimitDescription *node) {}

  /**
   * Visitor pattern for CreateStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::CreateStatement *node) {}

  /**
   * Visitor pattern for CreateFunctionStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::CreateFunctionStatement *node) {}

  /**
   * Visitor pattern for InsertStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::InsertStatement *node) {}

  /**
   * Visitor pattern for DeleteStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::DeleteStatement *node) {}

  /**
   * Visitor pattern for DropStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::DropStatement *node) {}

  /**
   * Visitor pattern for PrepareStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::PrepareStatement *node) {}

  /**
   * Visitor pattern for ExecuteStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::ExecuteStatement *node) {}

  /**
   * Visitor pattern for TransactionStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::TransactionStatement *node) {}

  /**
   * Visitor pattern for UpdateStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::UpdateStatement *node) {}

  /**
   * Visitor pattern for CopyStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::CopyStatement *node) {}

  /**
   * Visitor pattern for AnalyzeStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::AnalyzeStatement *node) {}

  /**
   * Visitor pattern for ExplainStatement.
   * @param node node to be visited
   */
  virtual void Visit(parser::ExplainStatement *node) {}
};

}  // namespace terrier
