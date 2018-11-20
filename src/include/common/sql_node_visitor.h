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
struct TableRef;

class GroupByDescription;
class OrderByDescription;
class LimitDescription;
}  // namespace parser

class SqlNodeVisitor {
 public:
  virtual ~SqlNodeVisitor() = default;

  virtual void Visit(parser::SelectStatement *node) {}

  // Some sub query nodes inside SelectStatement
  virtual void Visit(parser::JoinDefinition *node) {}
  virtual void Visit(parser::TableRef *node) {}
  virtual void Visit(parser::GroupByDescription *node) {}
  virtual void Visit(parser::OrderByDescription *node) {}
  virtual void Visit(parser::LimitDescription *node) {}

  virtual void Visit(parser::CreateStatement *node) {}
  virtual void Visit(parser::CreateFunctionStatement *node) {}
  virtual void Visit(parser::InsertStatement *node) {}
  virtual void Visit(parser::DeleteStatement *node) {}
  virtual void Visit(parser::DropStatement *node) {}
  virtual void Visit(parser::PrepareStatement *node) {}
  virtual void Visit(parser::ExecuteStatement *node) {}
  virtual void Visit(parser::TransactionStatement *node) {}
  virtual void Visit(parser::UpdateStatement *node) {}
  virtual void Visit(parser::CopyStatement *node) {}
  virtual void Visit(parser::AnalyzeStatement *node) {}
  virtual void Visit(parser::ExplainStatement *node) {}
};

}  // namespace terrier
