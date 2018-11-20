#pragma once

#include <sql/expression/sql_abstract_expression.h>

namespace terrier::parser {
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
class OrderDescription;
class LimitDescription;

class AbstractExpression;
class ComparisonExpression;
class AggregateExpression;
class ConjunctionExpression;
class ConstantValueExpression;
class OperatorExpression;
class ParameterValueExpression;
class StarExpression;
class TupleValueExpression;
class FunctionExpression;
class CaseExpression;
class SubqueryExpression;

class SqlNodeVisitor {
 public:
  virtual ~SqlNodeVisitor() = 0;
  virtual void Visit(JoinDefinition *def) {}
  virtual void Visit(TableRef *ref) {}
  virtual void Visit(GroupByDescription *desc) {}
  virtual void Visit(OrderDescription *desc) {}
  virtual void Visit(LimitDescription *desc) {}

  virtual void Visit(CreateStatement *statement) {}
  virtual void Visit(CreateFunctionStatement *statement) {}
  virtual void Visit(InsertStatement *statement) {}
  virtual void Visit(DeleteStatement *statement) {}
  virtual void Visit(DropStatement *statement) {}
  virtual void Visit(PrepareStatement *statement) {}
  virtual void Visit(ExecuteStatement *statement) {}
  virtual void Visit(TransactionStatement *statement) {}
  virtual void Visit(UpdateStatement *statement) {}
  virtual void Visit(CopyStatement *statement) {}
  virtual void Visit(AnalyzeStatement *statement){};
  virtual void Visit(ExplainStatement *statement){};

  virtual void Visit(ComparisonExpression *expr);
  virtual void Visit(AggregateExpression *expr);
  virtual void Visit(CaseExpression *expr);
  virtual void Visit(ConjunctionExpression *expr);
  virtual void Visit(ConstantValueExpression *expr);
  virtual void Visit(FunctionExpression *expr);
  virtual void Visit(OperatorExpression *expr);
  virtual void Visit(ParameterValueExpression *expr);
  virtual void Visit(StarExpression *expr);
  virtual void Visit(TupleValueExpression *expr);
  virtual void Visit(SubqueryExpression *expr);
};
}  // namespace terrier::parser
